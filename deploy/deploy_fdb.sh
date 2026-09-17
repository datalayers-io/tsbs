#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy_fdb.sh - install, bootstrap and join a 3-node FoundationDB cluster.
#
# Run as root on EACH of the 3 machines. Execution order does not matter:
# every machine starts its local fdbserver, waits until all 3 coordinators are
# reachable, and only then configures the database, so the machines converge on
# a single 3-node cluster no matter the order they were deployed.
#
# Usage:
#   sudo ./deploy_fdb.sh <ip1> <ip2> <ip3>
#   sudo ./deploy_fdb.sh --self <local_ip> <ip1> <ip2> <ip3>
#
# Behavior:
#   1. If FoundationDB is already running for this deployment (a fdbserver is
#      up using our cluster file), print the running version and the current
#      cluster status, then exit 0.
#   2. Detect distro/arch, ensure the matching FoundationDB packages are
#      installed (downloaded from GitHub releases if missing).
#   3. Write a per-machine foundationdb.conf (public-address = this machine),
#      install the SHARED cluster file listing all 3 coordinators, wipe any
#      stale local state, and start fdbmonitor.
#   4. Wait until all 3 coordinators are reachable, then create the database
#      (`configure new single ssd`), validate, and report cluster info.
#
# Env overrides (mainly for isolated testing / custom install roots):
#   FDB_VERSION  FDB_DOWNLOAD_BASE  PKG_DIR  FDB_PORT
#   FDB_CONF_FILE  FDB_CLUSTER_FILE  FDB_DATA_DIR  FDB_LOG_DIR
#   FDB_MONITOR_LOCK  FDB_MANAGE_MODE  FDB_WAIT_SECONDS  FDB_CLUSTER_DESC
# ---------------------------------------------------------------------------
set -euo pipefail

# --- Defaults ---------------------------------------------------------------
# FoundationDB version (matches docker/Dockerfile.ci). Override with FDB_VERSION.
FDB_VERSION="${FDB_VERSION:-7.3.76}"
FDB_DOWNLOAD_BASE="${FDB_DOWNLOAD_BASE:-https://github.com/apple/foundationdb/releases/download}"
PKG_DIR="${PKG_DIR:-/var/cache/foundationdb}"
FDB_PORT="${FDB_PORT:-4500}"
FDB_CONF_FILE="${FDB_CONF_FILE:-/etc/foundationdb/foundationdb.conf}"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
FDB_DATA_DIR="${FDB_DATA_DIR:-/var/lib/foundationdb/data}"
FDB_LOG_DIR="${FDB_LOG_DIR:-/var/log/foundationdb}"
FDB_MONITOR_LOCK="${FDB_MONITOR_LOCK:-/var/run/fdbmonitor.pid}"
FDB_WAIT_SECONDS="${FDB_WAIT_SECONDS:-600}"
FDB_CLUSTER_DESC="${FDB_CLUSTER_DESC:-fdb}"
FDB_SERVER_BIN="${FDB_SERVER_BIN:-$(command -v fdbserver || echo /usr/sbin/fdbserver)}"
FDB_CLI_BIN="${FDB_CLI_BIN:-$(command -v fdbcli || echo /usr/bin/fdbcli)}"
# FDB_MONITOR_BIN is resolved lazily (see fdbmonitor_bin) because it may not exist
# until the packages are installed below.
FDB_MONITOR_BIN="${FDB_MONITOR_BIN:-}"

log()  { echo "[deploy_fdb] $(date '+%F %T') $*"; }
warn() { echo "[deploy_fdb] WARNING: $*" >&2; }
die()  { echo "[deploy_fdb] ERROR: $*" >&2; exit 1; }

# --- Argument parsing -------------------------------------------------------
SELF_IP=""
NODE_IPS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --self) SELF_IP="${2:?--self requires an IP}"; shift 2;;
    -h|--help) sed -n '2,40p' "$0"; exit 0;;
    -*) die "unknown option: $1";;
    *) NODE_IPS+=("$1"); shift;;
  esac
done
[ "${#NODE_IPS[@]}" -eq 3 ] || die "usage: $0 [--self <local_ip>] <ip1> <ip2> <ip3> (got ${#NODE_IPS[@]} ips)"
[ "$(id -u)" -eq 0 ] || die "must run as root (use sudo)"

# --- System / arch detection ------------------------------------------------
detect_os() {
  if [ -f /etc/debian_version ] || command -v dpkg >/dev/null 2>&1; then
    OS_FAMILY="debian"
  elif [ -f /etc/redhat-release ] || command -v rpm >/dev/null 2>&1; then
    OS_FAMILY="rhel"
  else
    die "unsupported OS (only Debian/Ubuntu and CentOS/RHEL are supported)"
  fi

  local m
  m="$(uname -m)"
  case "$m" in
    x86_64|amd64)  ARCH="x86_64"; DEB_ARCH="amd64";;
    aarch64|arm64) ARCH="aarch64"; DEB_ARCH="aarch64";;
    *) die "unsupported architecture: $m (expected x86_64/amd64 or aarch64/arm64)";;
  esac
  # For FDB 7.3.x releases: el7 ships only x86_64, el9 ships only aarch64.
  # Override via FDB_CENTOS_VERSION if a different combination is desired.
  if [ "$ARCH" = "x86_64" ]; then CENTOS_VERSION="${FDB_CENTOS_VERSION:-el7}"; else CENTOS_VERSION="${FDB_CENTOS_VERSION:-el9}"; fi
  log "detected OS=$OS_FAMILY arch=$ARCH"
}

# --- Local IP detection -----------------------------------------------------
detect_local_ip() {
  if [ -n "$SELF_IP" ]; then
    LOCAL_IP="$SELF_IP"
    return 0
  fi
  local if_ips h cand
  if_ips="$(ip -o -4 addr show 2>/dev/null | awk '{print $4}' | cut -d/ -f1)"
  for cand in "${NODE_IPS[@]}"; do
    if grep -qx "$cand" <<<"$if_ips"; then LOCAL_IP="$cand"; return 0; fi
  done
  for h in $(hostname -I 2>/dev/null); do
    for cand in "${NODE_IPS[@]}"; do
      [ "$h" = "$cand" ] && { LOCAL_IP="$cand"; return 0; }
    done
  done
  return 1
}

# --- Shared cluster file ----------------------------------------------------
# The cluster file must be byte-identical on all 3 machines. Derive a
# deterministic cluster ID from the sorted list of IPs so every machine
# computes the same value regardless of the order IPs are passed in.
build_connection_string() {
  local sorted
  sorted="$(printf '%s\n' "${NODE_IPS[@]}" | sort)"
  local cid
  cid="$(printf '%s' "$sorted" | sha256sum | cut -c1-16)"
  local addrs=() ip
  for ip in $sorted; do addrs+=("${ip}:${FDB_PORT}"); done
  local IFS=,
  CONNECTION_STRING="${FDB_CLUSTER_DESC}:${cid}@${addrs[*]}"
}

# --- Running check ----------------------------------------------------------
fdb_pid_for_cluster_file() {
  local pid
  for pid in $(pgrep -x fdbserver 2>/dev/null || true); do
    if grep -Fq "$FDB_CLUSTER_FILE" "/proc/$pid/cmdline" 2>/dev/null; then
      echo "$pid"; return 0
    fi
  done
  return 1
}

report_cluster() {
  local cluster_line cid coord_list st nproc addr
  if [ -f "$FDB_CLUSTER_FILE" ]; then
    cluster_line="$(tail -1 "$FDB_CLUSTER_FILE")"
    cid="${cluster_line#*:}"; cid="${cid%@*}"
    coord_list="${cluster_line##*@}"
    echo "    cluster name     : ${FDB_CLUSTER_DESC}"
    echo "    cluster id       : ${cid}"
    echo "    coordinators     : ${coord_list//,/, }"
  else
    echo "    cluster file     : $FDB_CLUSTER_FILE (not present)"
  fi

  st="$(timeout 25 "$FDB_CLI_BIN" -C "$FDB_CLUSTER_FILE" --exec "status details" 2>/dev/null || true)"
  nproc="$(grep -oE 'FoundationDB processes - [0-9]+' <<<"$st" | grep -oE '[0-9]+' | head -1 || echo unknown)"
  echo "    fdb processes    : ${nproc:-unknown}"
  echo "    nodes:"
  while IFS= read -r addr; do
    [ -n "$addr" ] && echo "      $addr"
  done <<<"$(sed -n '/^Process performance details:/,/^Coordination servers:/p' <<<"$st" \
              | grep -oE '^  [0-9.]+:[0-9]+' | sed 's/^  //')"
  if [ -z "$(sed -n '/^Process performance details:/,/^Coordination servers:/p' <<<"$st" | grep -oE '^  [0-9.]+:[0-9]+')" ] && [ -n "$st" ]; then
    echo "      (status command did not report per-process details)"
  fi
}

print_running() {
  log "FoundationDB is already running on this host (fdbserver pid $1):"
  echo "    fdbcli version    : $("$FDB_CLI_BIN" -v 2>/dev/null | head -1)"
  echo "    fdbserver version : $("$FDB_SERVER_BIN" -v 2>/dev/null | head -1)"
  echo "    cluster file      : $FDB_CLUSTER_FILE"
  report_cluster
  exit 0
}

# --- Install ----------------------------------------------------------------
ensure_packages() {
  local file base="$FDB_DOWNLOAD_BASE/$FDB_VERSION" url
  local pkg_urls=()
  case "$OS_FAMILY" in
    debian)
      pkg_urls=(
        "$base/foundationdb-clients_${FDB_VERSION}-1_${DEB_ARCH}.deb"
        "$base/foundationdb-server_${FDB_VERSION}-1_${DEB_ARCH}.deb"
      );;
    rhel)
      pkg_urls=(
        "$base/foundationdb-clients-${FDB_VERSION}-1.${CENTOS_VERSION}.${ARCH}.rpm"
        "$base/foundationdb-server-${FDB_VERSION}-1.${CENTOS_VERSION}.${ARCH}.rpm"
      );;
  esac

  mkdir -p "$PKG_DIR"
  for url in "${pkg_urls[@]}"; do
    file="$PKG_DIR/${url##*/}"
    if [ -f "$file" ]; then
      log "package already present: $file"
    else
      log "downloading $url"
      curl -fsSL --retry 3 -o "$file" "$url" || die "download failed: $url"
      curl -fsSL --retry 3 -o "$file.sha256" "$url.sha256" || die "checksum download failed: $url.sha256"
    fi
    if [ -f "$file.sha256" ]; then
      ( cd "$PKG_DIR" && sha256sum -c "$(basename "$file").sha256" >/dev/null 2>&1 ) \
        || die "checksum verification failed for $file"
    else
      warn "no $file.sha256 for locally provided package; skipping checksum verification"
    fi
  done
}

# The server deb's postinst tries to auto-configure a single-node cluster with
# `fdbcli configure new single memory`. That races (and is incompatible with our
# multi-node shared cluster file) in containers, so dpkg may report an error even
# though the binaries are installed. We manage fdbmonitor ourselves, so tolerate it.
install_packages() {
  case "$OS_FAMILY" in
    debian)
      # fdbserver depends on fdbclient, so install clients first.
      dpkg -i "$PKG_DIR"/foundationdb-clients_*.deb
      dpkg -i "$PKG_DIR"/foundationdb-server_*.deb || {
        warn "dpkg reported an error for foundationdb-server (postinst auto-configure); binaries are installed, continuing"
      }
      ;;
    rhel)
      rpm -i "$PKG_DIR"/foundationdb-clients-*.rpm
      rpm -i "$PKG_DIR"/foundationdb-server-*.rpm || {
        warn "rpm reported an error for foundationdb-server (postinst auto-configure); binaries are installed, continuing"
      }
      ;;
  esac
  # Genuinely verify the binaries landed, regardless of the postinst result.
  [ -x "$FDB_SERVER_BIN" ] && [ -x "$FDB_CLI_BIN" ] \
    || die "FoundationDB binaries missing after install"
}

ensure_installed() {
  if [ -x "$FDB_SERVER_BIN" ] && [ -x "$FDB_CLI_BIN" ]; then
    local v
    v="$("$FDB_SERVER_BIN" -v 2>/dev/null | head -1)"
    log "found existing FoundationDB install: ${v:-unknown} (skipping download/install)"
    case "$v" in
      *"$FDB_VERSION"*) ;;
      *) warn "installed version does not match target $FDB_VERSION; using the installed binaries";;
    esac
    return 0
  fi
  log "FoundationDB not installed; downloading packages for $FDB_VERSION ($ARCH)"
  ensure_packages
  log "installing FoundationDB packages"
  install_packages
  [ -x "$FDB_SERVER_BIN" ] && [ -x "$FDB_CLI_BIN" ] || die "FoundationDB install did not provide fdbserver/fdbcli"
  log "installed: $("$FDB_SERVER_BIN" -v 2>/dev/null | head -1)"
}

# --- Config + cluster file --------------------------------------------------
write_config_and_cluster() {
  mkdir -p "$(dirname "$FDB_CONF_FILE")" "$FDB_DATA_DIR" "$FDB_LOG_DIR"
  if id foundationdb >/dev/null 2>&1; then
    chown -R foundationdb:foundationdb "$FDB_DATA_DIR" "$FDB_LOG_DIR" 2>/dev/null || true
  fi

  cat > "$FDB_CONF_FILE" <<EOF
## foundationdb.conf (generated by deploy_fdb.sh)
[fdbmonitor]
user = foundationdb
group = foundationdb

[general]
restart-delay = 5
cluster-file = $FDB_CLUSTER_FILE
kill-on-configuration-change = true

[fdbserver]
command = $FDB_SERVER_BIN
public-address = auto:\$ID
listen-address = public
datadir = $FDB_DATA_DIR/\$ID
logdir = $FDB_LOG_DIR
logsize = 10MiB
maxlogssize = 100MiB

[fdbserver.${FDB_PORT}]
public-address = ${LOCAL_IP}:${FDB_PORT}
listen-address = public
datadir = $FDB_DATA_DIR/${FDB_PORT}
logdir = $FDB_LOG_DIR
EOF

  # Shared, byte-identical cluster file on every machine.
  printf '%s\n' "$CONNECTION_STRING" > "$FDB_CLUSTER_FILE"
  chmod 0644 "$FDB_CLUSTER_FILE"
  log "wrote $FDB_CONF_FILE and shared cluster file $FDB_CLUSTER_FILE"
  log "connection string: $CONNECTION_STRING"
}

wipe_local_state() {
  log "wiping stale local FDB state under $FDB_DATA_DIR/${FDB_PORT} (fresh join)"
  rm -rf "${FDB_DATA_DIR:?}/${FDB_PORT:?}"
  mkdir -p "$FDB_DATA_DIR/${FDB_PORT}"
  if id foundationdb >/dev/null 2>&1; then
    chown -R foundationdb:foundationdb "$FDB_DATA_DIR/${FDB_PORT}" 2>/dev/null || true
  fi
}

# --- Service management -----------------------------------------------------
# fdbmonitor may not exist until the packages are installed, so resolve it lazily.
fdbmonitor_bin() {
  if [ -n "$FDB_MONITOR_BIN" ]; then
    echo "$FDB_MONITOR_BIN"
    return
  fi
  for p in /usr/lib/foundationdb/fdbmonitor /usr/sbin/fdbmonitor /usr/local/bin/fdbmonitor; do
    [ -x "$p" ] && { echo "$p"; return; }
  done
  echo "fdbmonitor"
}

resolve_manage_mode() {
  if [ -n "${FDB_MANAGE_MODE:-}" ]; then
    MANAGE_MODE="$FDB_MANAGE_MODE"
  elif [ "$FDB_CLUSTER_FILE" = "/etc/foundationdb/fdb.cluster" ] && \
       [ "$FDB_CONF_FILE" = "/etc/foundationdb/foundationdb.conf" ]; then
    MANAGE_MODE="service"
  else
    MANAGE_MODE="direct"
  fi
  log "fdbmonitor management mode: $MANAGE_MODE"
}

fdb_stop() {
  local pid
  case "$MANAGE_MODE" in
    service)
      service foundationdb stop >/dev/null 2>&1 || true
      ;;
    direct)
      if [ -f "$FDB_MONITOR_LOCK" ]; then
        kill "$(cat "$FDB_MONITOR_LOCK")" 2>/dev/null || true
      fi
      for pid in $(pgrep -x fdbserver 2>/dev/null || true); do
        if grep -Fq "$FDB_CLUSTER_FILE" "/proc/$pid/cmdline" 2>/dev/null; then
          kill "$pid" 2>/dev/null || true
        fi
      done
      ;;
  esac
  sleep 2
}

fdb_start() {
  case "$MANAGE_MODE" in
    service)
      service foundationdb start
      ;;
    direct)
      "$(fdbmonitor_bin)" --conffile "$FDB_CONF_FILE" --lockfile "$FDB_MONITOR_LOCK" --daemonize
      ;;
  esac
}

# --- Wait + configure -------------------------------------------------------
tcp_reachable() {
  local addr="$1" host port
  host="${addr%%:*}"; port="${addr##*:}"
  timeout 3 bash -c "</dev/tcp/$host/$port" 2>/dev/null
}

wait_all_reachable() {
  local end missing addr
  end=$(( $(date +%s) + FDB_WAIT_SECONDS ))
  while :; do
    missing=""
    for addr in "${COORD_ADDRS[@]}"; do
      if ! tcp_reachable "$addr"; then missing="${missing} ${addr}"; fi
    done
    if [ -z "$missing" ]; then
      log "all 3 coordinators reachable: ${COORD_ADDRS[*]}"
      return 0
    fi
    if [ "$(date +%s)" -ge "$end" ]; then
      warn "timed out after ${FDB_WAIT_SECONDS}s waiting for coordinators to come up. Missing:$missing"
      warn "local fdbserver keeps running; re-run this script on each remaining machine (or re-run here) to finish."
      return 1
    fi
    log "waiting for coordinators to become reachable (still down:$missing) ..."
    sleep 5
  done
}

configure_db() {
  local tries out
  tries=0
  while :; do
    out="$(timeout 25 "$FDB_CLI_BIN" -C "$FDB_CLUSTER_FILE" --exec "configure new single ssd" 2>&1 || true)"
    case "$out" in
      *"Database created"*) log "database created (configure new single ssd)"; return 0;;
      *"already exists"*)   log "database already exists on this cluster"; return 0;;
    esac
    tries=$((tries+1))
    [ "$tries" -ge 20 ] && { warn "could not configure database; last output: $(echo "$out" | tail -3)"; return 1; }
    sleep 3
  done
}

# --- Main -------------------------------------------------------------------
main() {
  detect_os
  detect_local_ip || die "could not determine this machine's IP among the provided nodes: ${NODE_IPS[*]} (use --self <local_ip>)"
  log "this machine ip: $LOCAL_IP"

  build_connection_string
  COORD_ADDRS=()
  local ip
  for ip in $(printf '%s\n' "${NODE_IPS[@]}" | sort); do COORD_ADDRS+=("${ip}:${FDB_PORT}"); done

  # 1) Already running -> report and exit.
  local running_pid
  running_pid="$(fdb_pid_for_cluster_file || true)"
  if [ -n "$running_pid" ]; then
    print_running "$running_pid"
  fi

  # 2) Install if needed.
  ensure_installed

  # 3) Stop any auto-started fdb, write our config + shared cluster file,
  #    wipe stale local state, then start fdbmonitor.
  resolve_manage_mode
  fdb_stop
  write_config_and_cluster
  wipe_local_state
  fdb_start

  # 4) Wait for all 3 coordinators, configure DB, validate.
  if wait_all_reachable; then
    configure_db || true
    log "validating cluster ..."
    sleep 5
    report_cluster
    log "done. This machine is part of the FoundationDB cluster."
  else
    warn "cluster not fully assembled on this run (some nodes still down)."
    warn "The local fdbserver is running; once the other 2 machines run this script the"
    warn "cluster will assemble automatically. Re-run this script on this machine later"
    warn "to print the final cluster state."
    exit 0
  fi
}

main "$@"

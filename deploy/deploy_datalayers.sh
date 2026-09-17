#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy_datalayers.sh - start a Datalayers node in cluster mode.
#
# Run on each cluster machine (one Datalayers node per machine).
#
# Usage:
#   sudo ./deploy_datalayers.sh
#   sudo ./deploy_datalayers.sh --binary /usr/bin/datalayers --config /etc/datalayers/datalayers.toml
#
# Behavior:
#   1. If any Datalayers service is already running on this host, print every
#      service: PID, command line and occupied listening ports, then exit 0.
#   2. Otherwise locate the `datalayers` binary (--binary, then common install
#      directories for Debian/Ubuntu and CentOS/RHEL). Error if not found.
#   3. Locate the config file (--config, default /etc/datalayers/datalayers.toml).
#      Error if not found.
#   4. Validate cluster-mode related settings in the config:
#        - server.standalone must not be true (error otherwise).
#        - warn if node.name is the default "localhost:8366".
#        - warn if the FoundationDB cluster file is missing.
#   5. Start datalayers in the background, print PID / binary / config / log.
# ---------------------------------------------------------------------------
set -euo pipefail

log()  { echo "[deploy_datalayers] $(date '+%F %T') $*"; }
warn() { echo "[deploy_datalayers] WARNING: $*" >&2; }
die()  { echo "[deploy_datalayers] ERROR: $*" >&2; exit 1; }

DEFAULT_CONFIG="/etc/datalayers/datalayers.toml"
DEFAULT_LOG_DIR="/var/log/datalayers"
DEFAULT_LOG_FILE="$DEFAULT_LOG_DIR/datalayers.log"

# --- Argument parsing -------------------------------------------------------
BINARY_ARG=""
CONFIG_ARG=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --binary) BINARY_ARG="${2:?--binary requires a path}"; shift 2;;
    --config) CONFIG_ARG="${2:?--config requires a path}"; shift 2;;
    -h|--help) sed -n '2,32p' "$0"; exit 0;;
    -*) die "unknown option: $1";;
    *) die "unexpected argument: $1 (use --binary/--config)";;
  esac
done

# --- 1) Detect already-running Datalayers -----------------------------------
detect_running() {
  local pid comm
  local found=0
  while read -r pid comm; do
    [ "$comm" = "datalayers" ] || continue
    found=1
    echo "  PID $pid  started by: $(ps -p "$pid" -o args= 2>/dev/null || echo '?')"
    echo "      listening ports:"
    if command -v ss >/dev/null 2>&1; then
      ss -ltnp 2>/dev/null | grep "pid=$pid" | awk '{print "        " $4}' | sort -u || true
    elif command -v netstat >/dev/null 2>&1; then
      netstat -ltnp 2>/dev/null | grep "$pid/" | awk '{print "        " $4}' | sort -u || true
    fi
  done < <(ps -eo pid=,comm=)

  if [ "$found" -eq 1 ]; then
    log "Datalayers service(s) already running on this host; printing them and exiting."
    exit 0
  fi
}

detect_running

# --- 2) Locate the datalayers binary ----------------------------------------
find_binary() {
  if [ -n "$BINARY_ARG" ]; then
    [ -f "$BINARY_ARG" ] && [ -x "$BINARY_ARG" ] || die "binary not found or not executable: $BINARY_ARG"
    echo "$BINARY_ARG"
    return
  fi
  local candidates=(
    "$(command -v datalayers 2>/dev/null || true)"
    "/usr/bin/datalayers"
    "/usr/local/bin/datalayers"
    "/opt/datalayers/bin/datalayers"
    "/usr/lib/datalayers/bin/datalayers"
    "$HOME/.local/bin/datalayers"
  )
  local c
  for c in "${candidates[@]}"; do
    [ -n "$c" ] && [ -x "$c" ] && { echo "$c"; return; }
  done
  die "no datalayers binary found. Searched: $(printf '%s, ' "${candidates[@]}")and --binary. Install the Datalayers package or pass --binary <path>."
}

BINARY="$(find_binary)"
log "datalayers binary: $BINARY"

# --- 3) Locate the config file ----------------------------------------------
find_config() {
  if [ -n "$CONFIG_ARG" ]; then
    [ -f "$CONFIG_ARG" ] || die "config file not found: $CONFIG_ARG"
    echo "$CONFIG_ARG"
    return
  fi
  [ -f "$DEFAULT_CONFIG" ] || die "no config file found at default location $DEFAULT_CONFIG (pass --config <path>)"
  echo "$DEFAULT_CONFIG"
}

CONFIG="$(find_config)"
log "config file: $CONFIG"

# --- 4) Validate cluster-mode related config --------------------------------
# server.standalone = true  => standalone mode; we must start in cluster mode.
if grep -Eq '^[[:space:]]*standalone[[:space:]]*=[[:space:]]*true([[:space:]]*(#.*)?)?$' "$CONFIG"; then
  die "config $CONFIG sets server.standalone = true (standalone mode). This script deploys a cluster node; set 'standalone = false' in [server] and try again."
fi

# node.name should be a unique <host>:<port> per node, not the default.
NODE_NAME="$(grep -E '^[[:space:]]*name[[:space:]]*=' "$CONFIG" | tail -1 || true)"
case "$NODE_NAME" in
  *'localhost:8366'*)
    warn "node.name in $CONFIG is the default 'localhost:8366'. In a real multi-node cluster every node needs a unique reachable name (e.g. '<ip>:8366')."
    ;;
esac

# Cluster mode relies on the FoundationDB cluster file.
if [ ! -f /etc/foundationdb/fdb.cluster ]; then
  warn "default FoundationDB cluster file /etc/foundationdb/fdb.cluster is missing; make sure the FoundationDB cluster is deployed before starting Datalayers."
fi

# Datalayers will refuse to start without a valid license (key or file).
if ! grep -Eq '^[[:space:]]*key[[:space:]]*=' "$CONFIG" && \
   ! grep -Eq '^[[:space:]]*file[[:space:]]*=' "$CONFIG"; then
  warn "no license key/file found in $CONFIG; the server will refuse to start until a valid [license] is configured."
fi

# --- 5) Start Datalayers in the background ----------------------------------
mkdir -p "$DEFAULT_LOG_DIR"
LOG_FILE="${LOG_FILE:-$DEFAULT_LOG_FILE}"

log "starting $BINARY -c $CONFIG in the background ..."
# Do not leak the admin shell's proxy settings into the daemon: an http_proxy in
# the environment would route local endpoints (e.g. a local MinIO object store)
# through the proxy and break them.
env -u http_proxy -u https_proxy -u all_proxy \
    -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    nohup "$BINARY" -c "$CONFIG" >> "$LOG_FILE" 2>&1 &
PID=$!

sleep 3
if ! kill -0 "$PID" 2>/dev/null; then
  die "datalayers exited shortly after start. Last log lines:\n$(tail -n 20 "$LOG_FILE" 2>/dev/null || true)"
fi

log "datalayers started."
echo "    pid        : $PID"
echo "    binary     : $BINARY"
echo "    config     : $CONFIG"
echo "    log file   : $LOG_FILE"

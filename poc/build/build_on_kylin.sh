#!/usr/bin/env bash
#
# build_on_kylin.sh - Build the Datalayers TSBS binaries on a bare-metal
# Kylin V10 (or any CentOS/RHEL-8-like) machine and pack a runnable bundle.
#
# Steps:
#   1) switch the Go module proxy to goproxy.cn (set GO_PROXY/GOPROXY)
#   2) install Go 1.22.x if missing or too old
#   3) install Go module dependencies (go mod download)
#   4) build with CGO (linked against the local glibc, so it runs on this box)
#   5) pack binaries + my_scripts/load_config into one runnable directory
#
# Usage:
#   ./build_on_kylin.sh                    # from the tsbs repo root
#   GO_VERSION=1.22.12 ./build_on_kylin.sh # pin a specific Go version
#   GO_PREFIX=$HOME/go ./build_on_kylin.sh # install Go under $HOME/go
#
# NOTE: building with CGO needs `gcc` and glibc-devel headers; the script
# tries to install them via dnf/yum when missing (requires root/sudo).
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${REPO_DIR}"

# Go module proxy (goproxy.cn first, fall back to direct).
export GO111MODULE=on
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOTOOLCHAIN=local

GO_VERSION="${GO_VERSION:-1.22.3}"
GO_PREFIX="${GO_PREFIX:-/usr/local}"
GO_HOME="${GO_PREFIX}/go"

# ── helpers ──────────────────────────────────────────────────────────────
info()  { echo "==> $*"; }
warn()  { echo "WARN: $*" >&2; }
die()   { echo "ERROR: $*" >&2; exit 1; }

run_as_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    return 1
  fi
}

ensure_cmd() {
  # ensure_cmd <cmd> <pkgs...>
  local cmd="$1"; shift
  if command -v "${cmd}" >/dev/null 2>&1; then
    return 0
  fi
  warn "${cmd} not found, trying to install: $*"
  if run_as_root dnf install -y "$@"; then
    return 0
  fi
  if run_as_root yum install -y "$@"; then
    return 0
  fi
  die "${cmd} is required but could not be installed. Install it manually and rerun."
}

# ── 1) prerequisites ─────────────────────────────────────────────────────
ensure_cmd curl curl
ensure_cmd make make
ensure_cmd gcc gcc glibc-devel

# ── 2) install / verify Go ───────────────────────────────────────────────
install_go() {
  local arch
  case "$(uname -m)" in
    x86_64)        arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) die "unsupported architecture: $(uname -m)" ;;
  esac

  local tarball="go${GO_VERSION}.linux-${arch}.tar.gz"
  local url="${GO_MIRROR:-https://mirrors.aliyun.com/golang}/${tarball}"
  local tmp
  tmp="$(mktemp -d)"
  info "Downloading Go ${GO_VERSION} from ${url} ..."
  curl -fL --retry 3 -o "${tmp}/${tarball}" "${url}" \
    || die "failed to download Go from ${url}; set GO_MIRROR to an alternate mirror"

  info "Installing Go to ${GO_HOME} ..."
  if [ -d "${GO_HOME}" ]; then
    run_as_root rm -rf "${GO_HOME}" \
      || die "cannot remove existing ${GO_HOME}; run as root or set GO_PREFIX to a writable path"
  fi
  if ! run_as_root mkdir -p "${GO_PREFIX}"; then
    die "cannot create ${GO_PREFIX}; run as root or set GO_PREFIX to a writable path"
  fi
  if run_as_root tar -C "${GO_PREFIX}" -xzf "${tmp}/${tarball}"; then
    :
  elif tar -C "${GO_PREFIX}" -xzf "${tmp}/${tarball}"; then
    :
  else
    rm -rf "${tmp}"
    die "failed to extract Go; run as root or set GO_PREFIX to a writable path"
  fi
  rm -rf "${tmp}"
  export PATH="${GO_HOME}/bin:${PATH}"
}

if command -v go >/dev/null 2>&1; then
  current="$(go env GOVERSION 2>/dev/null | sed 's/^go//')"
  if [ -n "${current}" ] && [ "$(printf '%s\n%s\n' "${current}" "${GO_VERSION}" | sort -V | head -1)" = "${GO_VERSION}" ]; then
    info "Using existing Go ${current} at $(command -v go)"
  else
    warn "existing Go ${current:-unknown} is older than ${GO_VERSION}; installing ${GO_VERSION}"
    install_go
  fi
else
  info "Go not found; installing ${GO_VERSION}"
  install_go
fi

command -v go >/dev/null 2>&1 || PATH="${GO_HOME}/bin:${PATH}"
go version

# ── 3) install Go module dependencies ────────────────────────────────────
info "Downloading Go module dependencies (GOPROXY=${GOPROXY}) ..."
go mod download

# ── 4) build ─────────────────────────────────────────────────────────────
info "Building Datalayers TSBS binaries (CGO enabled) ..."
make all

# ── 5) pack ──────────────────────────────────────────────────────────────
info "Packaging binaries + scripts ..."
./poc/scripts/pack.sh

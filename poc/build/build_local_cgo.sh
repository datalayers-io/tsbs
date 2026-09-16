#!/usr/bin/env bash
#
# build_local_cgo.sh - One-click build of every Datalayers-related TSBS binary
# with CGO enabled, then pack binaries + scripts into a runnable bundle.
#
# Usage:
#   ./build_local_cgo.sh            # build with CGO (default) and pack
#   CGO_ENABLED=0 ./build_local_cgo.sh   # force a fully static build
#
# WARNING: CGO binaries are dynamically linked against the local glibc. A
# bundle built on a distro with a newer glibc than the target (e.g. Ubuntu
# 26.04 glibc 2.43 -> Kylin V10 glibc 2.28) will NOT run on the target
# ("GLIBC_2.xx not found"). On a bare-metal Kylin V10 machine prefer
# ./build_on_kylin.sh instead.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${REPO_DIR}"

export GO111MODULE=on
export CGO_ENABLED="${CGO_ENABLED:-1}"

echo "==> Building Datalayers TSBS binaries (CGO_ENABLED=${CGO_ENABLED}) ..."
make all

echo "==> Packaging binaries + scripts ..."
./poc/scripts/pack.sh

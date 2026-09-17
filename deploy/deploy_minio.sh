#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy_minio.sh - 为 Datalayers 集群部署一个单实例 MinIO 容器，作为 S3 对象存储。
#
# 在【宿主机】执行（需要访问 docker），不要放进容器里跑。
#
# 两种网络模式：
#   1) 默认 bridge 模式（同机 3 容器测试）：MinIO 容器加入与 3 个 datalayers 容器相同的
#      docker 网络 (clusternet)，每个节点通过 http://172.20.0.20:9000 访问它。
#   2) host 模式（多台物理机 POC）：MINIO_NET=host，MinIO 直接绑定宿主机
#      <机器IP>:9000（与 datalayers 容器 --network host 同机，跨机器经宿主机 IP 互通）。
#
# Usage:
#   ./deploy_minio.sh              # 部署/幂等确认，并确保 bucket 存在
#   ./deploy_minio.sh --status     # 打印状态与连接信息
#   ./deploy_minio.sh --remove     # 停止并删除 MinIO 容器（保留数据卷）
#   ./deploy_minio.sh --remove --purge   # 连同数据卷一起删除
#
# Env overrides:
#   MINIO_IMAGE  MINIO_NAME  MINIO_NET  MINIO_NET_SUBNET  MINIO_IP
#   MINIO_API_PORT  MINIO_CONSOLE_PORT  MINIO_ROOT_USER  MINIO_ROOT_PASSWORD
#   MINIO_BUCKET  MINIO_DATA_VOL
# ---------------------------------------------------------------------------
set -euo pipefail

MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"
MINIO_NAME="${MINIO_NAME:-dl-minio}"
MINIO_NET="${MINIO_NET:-clusternet}"
MINIO_NET_SUBNET="${MINIO_NET_SUBNET:-172.20.0.0/24}"
MINIO_IP="${MINIO_IP:-172.20.0.20}"          # bridge 模式：容器固定 IP；host 模式：宿主机对外 IP（用于打印）
MINIO_API_PORT="${MINIO_API_PORT:-19000}"    # bridge: host 映射端口；host: MinIO 实际监听端口（默认应设 9000）
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-19001}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-datalayers}"
MINIO_DATA_VOL="${MINIO_DATA_VOL:-dl-minio-data}"

log()  { echo "[deploy_minio] $(date '+%F %T') $*"; }
warn() { echo "[deploy_minio] WARNING: $*" >&2; }
die()  { echo "[deploy_minio] ERROR: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker is required (run this script on the host)"

if [ "$MINIO_NET" = "host" ]; then
  HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  : "${MINIO_IP:=$HOST_IP}"
  [ -n "${MINIO_IP:-}" ] || MINIO_IP="127.0.0.1"
  ENDPOINT="http://${MINIO_IP}:${MINIO_API_PORT}"
  HEALTH_URL="http://127.0.0.1:${MINIO_API_PORT}/minio/health/ready"
  MC_NET_ARGS=(--network host)
  MC_URL="http://127.0.0.1:${MINIO_API_PORT}"
  HOST_LINE="    host 访问地址     : http://${MINIO_IP}:${MINIO_API_PORT}"
  CONSOLE_LINE="    console 地址      : http://${MINIO_IP}:${MINIO_CONSOLE_PORT}"
else
  ENDPOINT="http://${MINIO_IP}:9000"
  HEALTH_URL="http://127.0.0.1:${MINIO_API_PORT}/minio/health/ready"
  MC_NET_ARGS=(--network "$MINIO_NET")
  MC_URL="http://${MINIO_IP}:9000"
  HOST_LINE="    host 访问地址     : http://127.0.0.1:${MINIO_API_PORT}"
  CONSOLE_LINE="    console 地址      : http://127.0.0.1:${MINIO_CONSOLE_PORT}"
fi

ensure_network() {
  if [ "$MINIO_NET" = "host" ]; then
    return 0
  fi
  if ! docker network inspect "$MINIO_NET" >/dev/null 2>&1; then
    log "creating docker network $MINIO_NET ($MINIO_NET_SUBNET)"
    docker network create --driver bridge --subnet="$MINIO_NET_SUBNET" "$MINIO_NET"
  fi
}

wait_ready() {
  local tries=0
  until curl --noproxy '*' -sf "$HEALTH_URL" >/dev/null 2>&1; do
    tries=$((tries+1))
    [ "$tries" -ge 30 ] && { warn "MinIO not ready after 30s; check 'docker logs $MINIO_NAME'"; return 1; }
    sleep 1
  done
  log "MinIO is ready"
}

print_config() {
  echo
  echo "MinIO S3 连接信息："
  echo "    endpoint          : $ENDPOINT   (datalayers 容器内访问地址)"
  echo "$HOST_LINE"
  echo "$CONSOLE_LINE"
  echo "    access_key        : $MINIO_ROOT_USER"
  echo "    secret_key        : $MINIO_ROOT_PASSWORD"
  echo "    bucket            : $MINIO_BUCKET"
  echo
  echo "datalayers 节点配置中 [storage.object_store.s3] 应如下："
  cat <<EOF
[storage.object_store]
default_storage_type = "s3"

[storage.object_store.s3]
bucket = "$MINIO_BUCKET"
access_key = "$MINIO_ROOT_USER"
secret_key = "$MINIO_ROOT_PASSWORD"
endpoint = "$ENDPOINT"
region = "datalayers"
virtual_hosted_style = false
EOF
}

ensure_bucket() {
  log "ensuring bucket '$MINIO_BUCKET' exists"
  if command -v mc >/dev/null 2>&1; then
    mc alias set dl "$MC_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    mc mb --ignore-existing "dl/${MINIO_BUCKET}" >/dev/null
  else
    docker run --rm "${MC_NET_ARGS[@]}" "$MINIO_MC_IMAGE" alias set dl "$MC_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    docker run --rm "${MC_NET_ARGS[@]}" "$MINIO_MC_IMAGE" mb --ignore-existing "dl/${MINIO_BUCKET}" >/dev/null
  fi
  log "bucket '$MINIO_BUCKET' ready"
}

deploy() {
  ensure_network
  if docker inspect "$MINIO_NAME" >/dev/null 2>&1; then
    if [ "$(docker inspect -f '{{.State.Running}}' "$MINIO_NAME")" != "true" ]; then
      log "starting existing container $MINIO_NAME"
      docker start "$MINIO_NAME" >/dev/null
    else
      log "MinIO container $MINIO_NAME already running"
    fi
    wait_ready
    ensure_bucket
    print_config
    return 0
  fi

  log "starting MinIO container $MINIO_NAME (net=$MINIO_NET, endpoint=$ENDPOINT)"
  if [ "$MINIO_NET" = "host" ]; then
    docker run -d --name "$MINIO_NAME" \
      --network host \
      -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
      -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
      -v "${MINIO_DATA_VOL}:/data" \
      "$MINIO_IMAGE" server /data \
      --address ":${MINIO_API_PORT}" --console-address ":${MINIO_CONSOLE_PORT}" >/dev/null
  else
    docker run -d --name "$MINIO_NAME" \
      --network "$MINIO_NET" --ip "$MINIO_IP" \
      -p "${MINIO_API_PORT}:9000" -p "${MINIO_CONSOLE_PORT}:9001" \
      -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
      -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
      -v "${MINIO_DATA_VOL}:/data" \
      "$MINIO_IMAGE" server /data --console-address :9001 >/dev/null
  fi

  wait_ready
  ensure_bucket
  print_config
  log "MinIO deployed."
}

status() {
  if ! docker inspect "$MINIO_NAME" >/dev/null 2>&1; then
    echo "MinIO container '$MINIO_NAME' does not exist."
    return 0
  fi
  docker ps -a --filter "name=^${MINIO_NAME}$" --format "status: {{.Status}}  ports: {{.Ports}}"
  print_config
}

remove() {
  if docker inspect "$MINIO_NAME" >/dev/null 2>&1; then
    log "removing container $MINIO_NAME"
    docker rm -f "$MINIO_NAME" >/dev/null
  else
    log "container $MINIO_NAME not present"
  fi
  if [ "${PURGE:-0}" = "1" ]; then
    log "removing volume $MINIO_DATA_VOL"
    docker volume rm -f "$MINIO_DATA_VOL" >/dev/null 2>&1 || true
  fi
  log "done."
}

PURGE=0
case "${1:-}" in
  --status) status;;
  --remove)
    [ "${2:-}" = "--purge" ] && PURGE=1
    remove;;
  "") deploy;;
  *) die "unknown option: $1 (use --status / --remove [--purge])";;
esac

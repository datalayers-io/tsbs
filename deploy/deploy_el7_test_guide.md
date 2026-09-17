# Datalayers + FoundationDB 三节点部署测试指南（内部 · 170/172/180 · el7 / centos:7 容器）

内部测试用：在 170 / 172 / 180 三台机器上用 `centos:7` 容器（`--network host`）模拟，
验证 **el7 环境**的部署。端口使用非默认值，避免与机器上已有服务冲突。

> 连接用户：170 用 `nsc@192.168.1.170`，172 用 `nsq@192.168.1.172`，180 本机为 `nsc`（180 无需 ssh，直接在本机执行）。
> docker 命令若提示权限不足，前面加 `sudo`。
> **前置要求**：从 180 到 170/172 需已配置**免密 ssh**（否则把下面命令里的 `nsc@`/`nsq@` 换成你有权限的账号）。
> **Bundle 须为 el7 版**：`fdb_pkgs/` 放 el7 x86_64 rpm，datalayers/dlsql/dldump 为 el7 所构建。

端口规划（避开默认端口）：FDB `44500`、FlightSQL `18360`、HTTP `18361`、节点 RPC `18366`、
Prometheus `19090`、MinIO `19000/19001`。

### Bundle 内容（`poc_bundle.tar.gz`）

```
poc_bundle.tar.gz
├── datalayers            # Datalayers 服务端可执行文件（el7 兼容构建，glibc≤2.17）
├── dlsql                 # dlsql 命令行工具
├── dldump                # dldump 备份/恢复工具
├── deploy_fdb.sh         # FDB 部署脚本
├── deploy_datalayers.sh   # Datalayers 节点部署脚本
├── deploy_minio.sh       # MinIO 部署脚本（宿主机执行）
├── deploy_poc_el7_guide.md  # 部署文档（含 tsbs 压测 / el7 编译说明）
├── start_standalone_datalayers.sh  # 单机版 datalayers 后台启动脚本
├── fdb_pkgs/             # el7 x86_64 rpm（不含 .sha256）
└── configs/              # 节点集群配置 + 单机版配置
    ├── node1.toml  node2.toml  node3.toml
    └── standalone.config # 单机版（standalone=true, flight=28660/http=28661）
```

> `tsbs` 不在 bundle 内，压测时单独 clone（见第 5 步）。

---

## 0. 前置：清理、起容器、分发、进入容器（一次性，在 180 执行）

```bash
# 0.1 清理上一次测试环境（三台）
sudo docker rm -f node1 node2 node3 dl-minio 2>/dev/null
ssh nsc@192.168.1.170 'docker rm -f node1 node2 node3 dl-minio 2>/dev/null'
ssh nsq@192.168.1.172 'docker rm -f node1 node2 node3 dl-minio 2>/dev/null'

# 0.2 起三个 centos:7 容器（--network host）
sudo docker run -d --name node3 --network host --ulimit nofile=65535:65535 centos:7 sleep 1000000
ssh nsc@192.168.1.170 'docker run -d --name node1 --network host --ulimit nofile=65535:65535 centos:7 sleep 1000000'
ssh nsq@192.168.1.172 'docker run -d --name node2 --network host --ulimit nofile=65535:65535 centos:7 sleep 1000000'

# 0.3 分发 bundle 进各容器
sudo docker cp poc_bundle.tar.gz node3:/root/
scp poc_bundle.tar.gz nsc@192.168.1.170:/tmp/
ssh nsc@192.168.1.170 'docker cp /tmp/poc_bundle.tar.gz node1:/root/'
scp poc_bundle.tar.gz nsq@192.168.1.172:/tmp/
ssh nsq@192.168.1.172 'docker cp /tmp/poc_bundle.tar.gz node2:/root/'

# 0.4 180 宿主机也留一份（供 MinIO 脚本使用；tar 为系统自带，无需额外安装）
sudo cp poc_bundle.tar.gz /root/
sudo mkdir -p /root/deploy_host && sudo tar -xzf /root/poc_bundle.tar.gz -C /root/deploy_host

# 0.5 进入三个容器终端（打开三个终端，分别执行）
ssh nsc@192.168.1.170           # 终端A
docker exec -it node1 bash
ssh nsq@192.168.1.172           # 终端B
docker exec -it node2 bash
sudo docker exec -it node3 bash # 终端C（180）
```

进入容器后（三个容器终端分别执行）：

```bash
# centos:7 官方 yum 源已 EOL（mirrorlist 失效），先换成 aliyun 镜像（或 vault.centos.org）
rm -f /etc/yum.repos.d/CentOS-*.repo
cat > /etc/yum.repos.d/CentOS-Base.repo <<'EOF'
[base]
name=CentOS-$releasever - Base
baseurl=http://mirrors.aliyun.com/centos/$releasever/os/$basearch/
gpgcheck=0

[updates]
name=CentOS-$releasever - Updates
baseurl=http://mirrors.aliyun.com/centos/$releasever/updates/$basearch/
gpgcheck=0

[extras]
name=CentOS-$releasever - Extras
baseurl=http://mirrors.aliyun.com/centos/$releasever/extras/$basearch/
gpgcheck=0
EOF
yum clean all && yum makecache

# 装依赖 + 解压
yum install -y curl iproute procps-ng hostname which initscripts ca-certificates
mkdir -p /root/deploy && cd /root && tar -xzf poc_bundle.tar.gz -C /root/deploy
ls /root/deploy

# Python3 + pip（python 客户端示例用；el7 默认只有 python2，pyarrow20 需 python>=3.10）
curl -fsSL -o /tmp/py311.tar.gz "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.11.16%2B20260901-x86_64-unknown-linux-gnu-install_only.tar.gz"
mkdir -p /opt/python311 && tar -xzf /tmp/py311.tar.gz -C /opt/python311
export PATH=/opt/python311/python/bin:$PATH
git clone --depth 1 https://github.com/datalayers-io/examples.git /root/examples   # 需代理则先 git config --global http.proxy http://192.168.1.233:7890
python3 -m pip install --only-binary=:all: -i https://mirrors.aliyun.com/pypi/simple/ -r /root/examples/python/requirements.txt

# 把 datalayers/dlsql/dldump 安装到 PATH（无需路径前缀即可运行）
cp /root/deploy/datalayers /root/deploy/dlsql /root/deploy/dldump /usr/local/bin/
export PATH="/usr/local/bin:$PATH"
command -v datalayers dlsql dldump
```

> 后续步骤都在【容器终端】内执行。

准备环境变量并生成 configs（容器终端内）：

```bash
export M1_IP=192.168.1.170 M2_IP=192.168.1.172 M3_IP=192.168.1.180
export MINIO_HOST=192.168.1.180 MINIO_API_PORT=19000
cd /root/deploy
for n in 1 2 3; do
  f=configs/node${n}.toml
  sed -e "s|__M1_IP__|$M1_IP|g" -e "s|__M2_IP__|$M2_IP|g" -e "s|__M3_IP__|$M3_IP|g" \
      -e "s|__MINIO_ENDPOINT__|http://${MINIO_HOST}:${MINIO_API_PORT}|g" \
      "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
grep -h '^name' configs/node*.toml   # 期望 $M1_IP:18366 / $M2_IP:18366 / $M3_IP:18366
```

## 1. 检查端口空闲（容器终端，三个都执行）

```bash
ss -ltn | grep -E ":(44500|18360|18361|18366|19090)\b" || echo ports-free
```

> 若 180 已跑 MinIO（19000/19001），由第 3 步确认，无需在此检查。

## 2. 部署 FDB（容器终端，三个都执行，任意顺序）

```bash
cd /root/deploy
FDB_MANAGE_MODE=direct FDB_PORT=44500 PKG_DIR=/root/deploy/fdb_pkgs FDB_WAIT_SECONDS=240 \
  bash ./deploy_fdb.sh "$M1_IP" "$M2_IP" "$M3_IP"
```

校验（容器终端）：

```bash
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status minimal"
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status" | grep -E "FoundationDB processes|Coordinators"
# 期望：The database is available.；FoundationDB processes - 3，Coordinators - 3
```

> 容器内没有 systemd，用 `FDB_MANAGE_MODE=direct`。若 `fdbmonitor` 报 inotify 超限，在宿主机
> `sudo sysctl -w fs.inotify.max_user_instances=1000000`（170/172 经 ssh 执行）。

## 3. 部署 MinIO（180 宿主机终端，不要进容器）

```bash
cd /root/deploy_host
MINIO_NET=host MINIO_API_PORT=19000 MINIO_CONSOLE_PORT=19001 MINIO_IP=192.168.1.180 \
  bash ./deploy_minio.sh
```

> 若默认 `minio/minio:latest` 拉取失败（如镜像加速器对 docker.io 403），加
> `MINIO_IMAGE=quay.io/minio/minio:latest`（或用本地已缓存镜像）；`deploy_minio.sh` 已支持
> 优先使用宿主机 `/usr/local/bin/mc`（不存在时才用 mc 容器建 bucket）。

校验（从 170 或 172 容器终端）：

```bash
curl --noproxy '*' -sf http://192.168.1.180:19000/minio/health/ready && echo minio-ok
```

## 4. 部署 datalayers（容器终端，三个分别执行对应配置）

```bash
cd /root/deploy
# 终端A（node1 / 170）
bash ./deploy_datalayers.sh --binary /root/deploy/datalayers --config /root/deploy/configs/node1.toml
# 终端B（node2 / 172）
bash ./deploy_datalayers.sh --binary /root/deploy/datalayers --config /root/deploy/configs/node2.toml
# 终端C（node3 / 180）
bash ./deploy_datalayers.sh --binary /root/deploy/datalayers --config /root/deploy/configs/node3.toml
```

校验（终端A）：

```bash
cd /root/deploy
./dlsql -h 127.0.0.1 -P 18360 -u admin -p public -e "SELECT 1"
./dlsql -h 127.0.0.1 -P 18360 -u admin -p public -e "CREATE DATABASE IF NOT EXISTS smoke_db; SHOW DATABASES;"
./dlsql -h 192.168.1.172 -P 18360 -u admin -p public -e "SHOW DATABASES;"   # 跨机器，期望看到 smoke_db
```

## 5. 部署单机版datalayers（可选验证）

Bundle 内含 `configs/standalone.config`（standalone=true，flight=28660/http=28661，不依赖集群）
与 `start_standalone_datalayers.sh`。任选一个容器终端执行：

```bash
bash /root/deploy/start_standalone_datalayers.sh /root/deploy/configs/standalone.config
# 输出：成功启动单机版 datalayers / flight sql 端口为: 28660 / http 端口为: 28661 / pid 为: <pid>
dlsql -h 127.0.0.1 -P 28660 -u admin -p public -e "SELECT 1"   # 验证单机版
```

---

## 6. TSBS 压测冒烟（容器终端，任选一台机器，如终端A/node1）

模拟客户真实环境，在 el7 容器内 clone、编译并跑 tsbs smoke 测试：

```bash
# 5.1 安装构建工具 + Go
yum install -y make gcc glibc-devel git
curl -fsSL -o /tmp/go.tar.gz https://mirrors.aliyun.com/golang/go1.22.3.linux-amd64.tar.gz
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=/usr/local/go/bin:$PATH

# 5.2 clone tsbs（git 走代理则先配置 http.proxy）
git config --global http.proxy http://192.168.1.233:7890
git config --global https.proxy http://192.168.1.233:7890
cd /root && git clone --depth 1 https://github.com/datalayers-io/tsbs.git

# 5.3 编译 tsbs（产物在 bin/）
cd /root/tsbs && GOPROXY=https://goproxy.cn,direct make all
ls bin/

# 5.4 配置三个 yaml（服务地址 + dlsql/dldump 目录）
cd /root/tsbs/poc
for f in bench_config.yaml bench_load_config.yaml bench_query_config.yaml; do
  sed -i -e "s|^flight_addr:.*|flight_addr: $M1_IP:18360|" \
         -e "s|^http_addr:.*|http_addr: $M1_IP:18361|" \
         -e "s|^dlsql_dir:.*|dlsql_dir: /root/deploy|" \
         -e "s|^dldump_dir:.*|dldump_dir: /root/deploy|" "$f"
done

# 5.5 冒烟测试（小规模全流程：建库建表→生成数据→灌 fresh+stale→15 种查询）
./bench.sh smoke
```

---

## 7. 常见问题

| 现象 | 处理 |
|------|------|
| `yum install` 报仓库 404 / mirrorlist 失效 | centos:7 官方源已 EOL，按 §0.5 换成 aliyun 源（或 vault.centos.org）后 `yum clean all && yum makecache` |
| 二进制报 `GLIBC_2.x not found` | bundle 二进制非 el7 兼容构建；需 el7 版 bundle 或改用 ubuntu24 测试指南 |
| `datalayers` 报找不到 `libfdb_c.so` | 容器内 `export LD_LIBRARY_PATH=/usr/lib64` 后启动 |
| `fdbmonitor` 报 inotify 超限 | 宿主机 `sysctl -w fs.inotify.max_user_instances=1000000` |
| `foundationdb-server` rpm 安装 postinst 报错 | 属预期（无 systemd）；二进制已装好，脚本会继续，重跑即可 |
| datalayers 无法连接 S3 | 确认 MinIO 在 180 已起（`curl --noproxy '*' http://192.168.1.180:19000/minio/health/ready`）；核对 configs 的 endpoint/ak/sk/bucket |
| dlsql 认证失败 | 默认 `admin/public`；configs 改了密码则调整 `-u/-p` |

---

## 8. 清理（可选，为下次部署准备干净环境）

> 只停掉部署的组件，**不删除** `poc_bundle`、`/root/deploy`、宿主机 `/root/deploy_host` 及已安装的包。

三个容器终端分别执行：

```bash
pkill -x datalayers 2>/dev/null || true
pkill -x fdbmonitor 2>/dev/null || true
pkill -x fdbserver  2>/dev/null || true
```

180 宿主机（MinIO）：

```bash
sudo docker rm -f dl-minio 2>/dev/null || true
```

> 若要彻底重置（连容器一起清掉），直接 `docker rm -f node1 node2 node3 dl-minio`（§0.1 会重建）。

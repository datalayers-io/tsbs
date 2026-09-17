# Datalayers + FoundationDB 三节点集群部署 POC 指南（正式 · el7）

面向**客户真实环境（CentOS/RHEL 7，el7）**的正式部署文档：直接跑在客户机器上，
不依赖 docker。节点 IP 等参数用**环境变量**定义，文档可复用。

- 架构：3 台机器各部署 1× fdbserver + 1× datalayers；其中一台（M3）可选部署 MinIO。
- 元数据存 FDB，数据文件存 S3（MinIO，或客户自备的 S3 协议 OBS）。
- 三个节点可**以任意顺序**部署，最终收敛成一个 3 节点集群。
- 每个部署步骤末尾自带校验命令。

---

## 0. 环境变量（部署前 export）

在**工作机**和**三台目标机器**上设置（按实际值修改）：

```bash
export M1_IP=192.168.1.170      # 机器1 内网 IP
export M2_IP=192.168.1.172      # 机器2 内网 IP
export M3_IP=192.168.1.180      # 机器3 内网 IP（MinIO / OBS 所在）
export FDB_PORT=4500            # FDB fdbserver 端口（默认 4500）
export MINIO_HOST=$M3_IP        # MinIO 所在机器 IP
export MINIO_API_PORT=9000      # MinIO API 端口（默认 9000）
export MINIO_CONSOLE_PORT=9001  # MinIO 控制台端口
export BUNDLE=/root/poc_bundle.tar.gz   # Bundle 在机器上的路径
export DEPLOY_DIR=/root/deploy          # 解压目录
```

> - 三台机器需互相可达，防火墙/安全组放行 `$FDB_PORT`、8360、8361、8366、9090，
>   以及跑 MinIO 机器的 `$MINIO_API_PORT` / `$MINIO_CONSOLE_PORT`。
> - Bundle 中 `configs/nodeN.toml` 使用占位符：`__M1_IP__`、`__M2_IP__`、`__M3_IP__`、
>   `__MINIO_ENDPOINT__`；部署前用第 2 步的命令替换。
> - Bundle 须为 **el7 版**：`fdb_pkgs/` 放 el7 x86_64 的 rpm（无需带 `.sha256`，deploy_fdb.sh
>   对本地无校验文件的包会跳过校验），datalayers/dlsql/dldump 为 el7 兼容构建（glibc ≤ 2.17，用 `ldd` / `strings ... | grep GLIBC_` 校验）。

### Bundle 内容（`poc_bundle.tar.gz`）

```
poc_bundle.tar.gz
├── datalayers            # Datalayers 服务端可执行文件（el7 兼容构建）
├── dlsql                 # dlsql 命令行工具
├── dldump                # dldump 备份/恢复工具
├── deploy_fdb.sh         # FDB 部署脚本（每台机器执行）
├── deploy_datalayers.sh   # Datalayers 节点部署脚本（每台机器执行）
├── deploy_minio.sh       # MinIO 部署脚本（宿主机执行，可选）
├── deploy_poc_el7_guide.md  # 部署文档（含 tsbs 压测 / el7 编译说明）
├── start_standalone_datalayers.sh  # 单机版 datalayers 后台启动脚本
├── fdb_pkgs/             # 对应系统的 FDB 安装包（el7 x86_64 rpm；不含 .sha256）
└── configs/              # 三个节点的集群版配置
    ├── node1.toml        #   node.name = __M1_IP__:8366
    ├── node2.toml        #   node.name = __M2_IP__:8366
    ├── node3.toml        #   node.name = __M3_IP__:8366
    │                     #   s3 endpoint = __MINIO_ENDPOINT__
    └── standalone.config #   单机版配置（standalone=true, flight=28660/http=28661）
```

> `tsbs` 不在 bundle 内，压测时单独 clone（见第 8 步）。

## 1. 前置：连接服务器、分发 Bundle（一次性）

打开三个终端，分别 ssh 到三台机器（客户机器连接信息以实际为准）：

```bash
ssh <user>@<machine1-ip>
ssh <user>@<machine2-ip>
ssh <user>@<machine3-ip>
```

在工作机终端把 Bundle 分发到三台机器：

```bash
scp poc_bundle.tar.gz <user>@<machine1-ip>:/root/
scp poc_bundle.tar.gz <user>@<machine2-ip>:/root/
scp poc_bundle.tar.gz <user>@<machine3-ip>:/root/
```

> 后续所有步骤都在对应机器的**终端内**执行（root 权限），不再出现 ssh 命令。

## 2. 拷 Bundle 并解压、准备 configs（每台机器，root）

```bash
export M1_IP M2_IP M3_IP FDB_PORT MINIO_HOST MINIO_API_PORT MINIO_CONSOLE_PORT BUNDLE DEPLOY_DIR   # 若换了终端重新设置
mkdir -p "$DEPLOY_DIR"
cd /root && tar -xzf "$BUNDLE" -C "$DEPLOY_DIR"
ls "$DEPLOY_DIR"   # 应含 datalayers dlsql dldump deploy_fdb.sh deploy_datalayers.sh deploy_minio.sh fdb_pkgs configs
```

用环境变量替换 configs 中的占位符：

```bash
cd "$DEPLOY_DIR"
for n in 1 2 3; do
  f=configs/node${n}.toml
  sed -e "s|__M1_IP__|$M1_IP|g" -e "s|__M2_IP__|$M2_IP|g" -e "s|__M3_IP__|$M3_IP|g" \
      -e "s|__MINIO_ENDPOINT__|http://${MINIO_HOST}:${MINIO_API_PORT}|g" \
      "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
grep -h '^name' "$DEPLOY_DIR"/configs/node*.toml   # 校验：应为 $M1_IP:8366 / $M2_IP:8366 / $M3_IP:8366
```

校验：目录结构完整、占位符已被替换。

把 `datalayers`/`dlsql`/`dldump` 安装到 PATH（无需路径前缀即可运行）：

```bash
cp "$DEPLOY_DIR"/datalayers "$DEPLOY_DIR"/dlsql "$DEPLOY_DIR"/dldump /usr/local/bin/
export PATH="/usr/local/bin:$PATH"
command -v datalayers dlsql dldump
```

> `datalayers` 依赖 `libfdb_c.so`：第 5 步装好 FDB 后 ldconfig 已注册 /usr/lib64；
> 若仍报找不到，先 `export LD_LIBRARY_PATH=/usr/lib64`。

## 3. 安装依赖（每台机器，root，el7）

```bash
yum install -y curl iproute procps-ng hostname which
```

> **CentOS 7 官方源已 EOL**：若 `yum install` 报 mirrorlist/仓库 404，先换 aliyun 或
> vault.centos.org 镜像源再装，例如写一份 `/etc/yum.repos.d/CentOS-Base.repo`（base/updates/extras，
> `baseurl=http://mirrors.aliyun.com/centos/$releasever/{os,updates,extras}/$basearch/`，`gpgcheck=0`），
> 删掉失效的 `CentOS-*.repo`，然后 `yum clean all && yum makecache`。

校验：

```bash
command -v curl ip pgrep hostname
```

**Python3 + pip（python 客户端示例用；el7 默认只有 python2）**：pyarrow 20 需 python>=3.10 且
仅提供 manylinux2014(glibc 2.17) wheel，推荐用自包含的 python-build-standalone（也可用 conda 旧版）：

```bash
# 下载 python 3.11（glibc 2.17 兼容；github 需代理时先 export https_proxy=http://<proxy>）
curl -fsSL -o /tmp/py311.tar.gz "https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.11.16%2B20260901-x86_64-unknown-linux-gnu-install_only.tar.gz"
mkdir -p /opt/python311 && tar -xzf /tmp/py311.tar.gz -C /opt/python311
export PATH=/opt/python311/python/bin:$PATH
python3 --version && python3 -m pip --version

# 拉取 examples 仓库并安装 python 依赖（requirements.txt 已按 el7 适配）
git clone --depth 1 https://github.com/datalayers-io/examples.git /root/examples
python3 -m pip install --only-binary=:all: -i https://mirrors.aliyun.com/pypi/simple/ -r /root/examples/python/requirements.txt
```

> requirements.txt 说明：`pyarrow==20.0.0`（最后一个 manylinux2014 wheel 版本，pyarrow>=21 需 glibc 2.28）、
> `numpy==1.26.4`（numpy 2.x 的 cp3x wheel 也是 manylinux_2_28）、`pandas==2.2.3`、
> `flightsql-dbapi==0.2.2`（pyarrow.flight 只有底层 RPC，无高层 FlightSQL，仍需它）、
> `protobuf==4.25.3`、`sqlalchemy==1.4.54`；安装务必加 `--only-binary=:all:` 避免源码编译。

## 4. 检查端口空闲（每台机器）

```bash
ss -ltn | grep -E ":(${FDB_PORT}|8360|8361|8366|9090)\b" || echo ports-free
# 机器3（跑 MinIO）额外检查：
ss -ltn | grep -E ":(${MINIO_API_PORT}|${MINIO_CONSOLE_PORT})\b" || echo ports-free
```

校验：输出 `ports-free`；否则停掉占用服务，或改用非默认端口（同步修改 `$FDB_PORT` 等环境变量并重新执行第 2 步生成 configs）。

## 5. 部署 FDB（每台机器，root，任意顺序）

```bash
cd "$DEPLOY_DIR"
PKG_DIR="$DEPLOY_DIR/fdb_pkgs" FDB_PORT=$FDB_PORT FDB_WAIT_SECONDS=300 \
  bash ./deploy_fdb.sh "$M1_IP" "$M2_IP" "$M3_IP"
```

> 每台只启动自己那一个 fdbserver（`<Mx_IP>:$FDB_PORT`），三台通过相同 coordinator 组
> 收敛成一个 3 节点 FDB 集群。第一个执行会阻塞等待三台 coordinators 可达（`FDB_WAIT_SECONDS`）。

校验（每台机器）：

```bash
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status minimal"     # 期望：The database is available.
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status" | grep -E "FoundationDB processes|Coordinators"
# 期望：FoundationDB processes - 3，Coordinators - 3
```

> 若 fdbmonitor 报 `inotify_init ... Too many open files`，先
> `sysctl -w fs.inotify.max_user_instances=1000000`（并写入 `/etc/sysctl.d/`）。

## 6. 部署 MinIO（可选，机器3，root）

若客户已提供 **S3 协议 OBS**，跳过本步，直接把 OBS 的 `endpoint / access_key / secret_key / bucket`
填入 `configs/node*.toml` 的 `[storage.object_store.s3]`。

在机器3 终端：

```bash
cd "$DEPLOY_DIR"
MINIO_NET=host MINIO_API_PORT=$MINIO_API_PORT MINIO_CONSOLE_PORT=$MINIO_CONSOLE_PORT \
  MINIO_IP=$MINIO_HOST bash ./deploy_minio.sh
```

> `deploy_minio.sh` 需要机器3 有 docker；若无 docker，直接运行 minio 二进制（见常见问题）。
> 若默认 `minio/minio:latest` 拉取失败（如镜像加速器对 docker.io 返回 403），加
> `MINIO_IMAGE=quay.io/minio/minio:latest`（或本地已缓存镜像）；脚本优先使用宿主机
> `/usr/local/bin/mc`（不存在时才用 mc 容器建 bucket）。
> 脚本会打印 `[storage.object_store.s3]` 所需配置，请把 endpoint / ak / sk / bucket 填入三份 configs。

校验（从机器1 或机器2 终端）：

```bash
curl --noproxy '*' -sf "http://${MINIO_HOST}:${MINIO_API_PORT}/minio/health/ready" && echo minio-ok
```

## 7. 部署 datalayers（每台机器，root）

机器1：

```bash
cd "$DEPLOY_DIR"
bash ./deploy_datalayers.sh --binary "$DEPLOY_DIR/datalayers" --config "$DEPLOY_DIR/configs/node1.toml"
```

机器2：同上，`--config .../configs/node2.toml`；机器3：`--config .../configs/node3.toml`。

校验（机器1 终端）：

```bash
cd "$DEPLOY_DIR"
./dlsql -h 127.0.0.1 -P 8360 -u admin -p public -e "SELECT 1"          # 期望返回 1
./dlsql -h 127.0.0.1 -P 8360 -u admin -p public -e "CREATE DATABASE IF NOT EXISTS smoke_db; SHOW DATABASES;"
./dlsql -h "$M2_IP" -P 8360 -u admin -p public -e "SHOW DATABASES;"   # 跨机器访问，期望能看到 smoke_db
```

## 8. 单机版（standalone）datalayers（可选验证）

Bundle 内含 `configs/standalone.config`（单机版配置：`standalone=true`、flight=28660、http=28661、
本地元数据/对象存储，不依赖 FDB 集群）与 `start_standalone_datalayers.sh`（后台启动并打印端口/pid）。

任选一台机器执行（`datalayers` 已在 PATH）：

```bash
bash "$DEPLOY_DIR/start_standalone_datalayers.sh" "$DEPLOY_DIR/configs/standalone.config"
# 输出：
#   成功启动单机版 datalayers
#     flight sql 端口为: 28660
#     http 端口为: 28661
#     pid 为: <pid>
#    日志: /var/log/datalayers-standalone/standalone.log

# 用 dlsql 验证单机版
dlsql -h 127.0.0.1 -P 28660 -u admin -p public -e "SELECT 1"
```

> 单机版与集群版是相互独立的 datalayers 进程，端口/数据目录各自独立；
> `datalayers` 二进制即使单机模式也依赖 `libfdb_c.so` 可加载（FDB 装好后在 /usr/lib64）。

## 9. TSBS 压测冒烟（在压测服务器上）

模拟客户真实环境做冒烟测试。在任一台机器上执行：

```bash
# 8.1 安装构建工具 + Go
yum install -y make gcc glibc-devel git
curl -fsSL -o /tmp/go.tar.gz https://mirrors.aliyun.com/golang/go1.22.3.linux-amd64.tar.gz
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=/usr/local/go/bin:$PATH

# 8.2 clone tsbs
#
#（git 走代理则先配置 http.proxy）
# git config --global http.proxy http://<proxy>
# git config --global https.proxy http://<proxy>
#
cd /root && git clone --depth 1 https://github.com/datalayers-io/tsbs.git

# 8.3 编译 tsbs（产物在 bin/）
cd /root/tsbs && GOPROXY=https://goproxy.cn,direct make all
ls bin/

# 8.4 配置三个 yaml（服务地址 + dlsql/dldump 目录；端口用部署时配置的）
cd /root/tsbs/poc
for f in bench_config.yaml bench_load_config.yaml bench_query_config.yaml; do
  sed -i -e "s|^flight_addr:.*|flight_addr: ${M1_IP}:8360|" \
         -e "s|^http_addr:.*|http_addr: ${M1_IP}:8361|" \
         -e "s|^dlsql_dir:.*|dlsql_dir: ${DEPLOY_DIR}|" \
         -e "s|^dldump_dir:.*|dldump_dir: ${DEPLOY_DIR}|" "$f"
done

# 8.5 冒烟测试（小规模全流程：建库建表→生成数据→灌 fresh+stale→15 种查询）
./bench.sh smoke
```

---

## 10. 常见问题

| 现象 | 处理 |
|------|------|
| `yum install` 报仓库 404 / mirrorlist 失效 | CentOS 7 官方源已 EOL；按第 3 步换 aliyun / vault 源后 `yum clean all && yum makecache` |
| 二进制报 `GLIBC_2.x not found` / 缺库 | bundle 二进制与 el7（glibc 2.17）不匹配，需 el7 兼容构建（见附录） |
| `datalayers` 报找不到 `libfdb_c.so` | `export LD_LIBRARY_PATH=/usr/lib64` 后启动（el7 clients rpm 装于此） |
| `fdbmonitor` 报 inotify 超限 | `sysctl -w fs.inotify.max_user_instances=1000000` 并持久化 |
| datalayers 无法连接 S3 | `curl --noproxy '*' http://${MINIO_HOST}:${MINIO_API_PORT}/minio/health/ready`；核对 configs 的 endpoint/ak/sk/bucket |
| 机器3 无 docker 部署 MinIO | 下载 minio 二进制：`wget https://dl.min.io/server/minio/release/linux-amd64/minio && MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /data --address :$MINIO_API_PORT --console-address :$MINIO_CONSOLE_PORT` |
| dlsql 认证失败 | 默认 `admin/public`；若 configs 改了 auth 密码，调整 `-u/-p` |

---

## 11. 附录：用 builder-el7 编译 el7 兼容二进制（datalayers / dlsql / dldump）

仓库 CI 使用 `ghcr.io/datalayers-io/datalayers/builder-el7:latest`（centos:7 + devtoolset-8 + protoc + FDB 头）编译 el7 版：

```bash
# 方式一：走仓库脚本（release/tools profile，产出 out/ 目录）
docker run --rm -v "$(pwd):/datalayers" -w /datalayers \
  --env STANDALONE=false \
  ghcr.io/datalayers-io/datalayers/builder-el7:latest \
  sh -c "source /opt/rh/devtoolset-8/enable; source /opt/rh/llvm-toolset-7.0/enable; \
         bash ./.github/package/build_scripts/build_datalayers.sh false"
# 产物：out/datalayers、out/dlsql、out/dldump

# 方式二：只编二进制，用 reldev profile（与开发构建一致；target 目录挂载为 /target）
docker run --rm -v "$(pwd):/datalayers" -w /datalayers \
  -v "$(pwd)/target_el7:/target" --env CARGO_TARGET_DIR=/target \
  ghcr.io/datalayers-io/datalayers/builder-el7:latest \
  bash -c 'source /opt/rh/devtoolset-8/enable; source /opt/rh/llvm-toolset-7.0/enable; \
           export PATH=/root/.cargo/bin:$PATH; \
           git config --global http.proxy http://<proxy>; git config --global https.proxy http://<proxy>; \
           cargo build --locked --profile reldev --bin datalayers && \
           cargo build --locked --profile reldev --bin dlsql --bin dldump --no-default-features --features "dldump web-console"'
# 产物：target_el7/reldev/{datalayers,dlsql,dldump}
```

编译坑与注意：

- rustup 会按仓库 `rust-toolchain.toml` 自动安装工具链（需网络），镜像内默认无 toolchain。
- **`CARGO_TARGET_DIR` 目录名必须以 `target` 结尾**：`src/dbserver/../build.rs` 用
  `.ancestors().find(|p| p.ends_with("target"))` 定位仓库根，用 `target_el7` 会 panic；建议挂载为 `/target`。
- 拉取 git 依赖（`datafusion-sqlparser-rs`）直连 github 可能 SSL 失败：容器内
  `git config --global http.proxy http://<proxy>`（或保证网络可达）。
- 校验 glibc：`strings ./datalayers | grep GLIBC_ | sort -V | tail -1` 应为 2.17 或更低。
- el7 版二进制（glibc ≤ 2.17）在 Ubuntu 24.04 等新系统上也能直接运行。

---

## 12. 清理（可选，为下次部署准备干净环境）

> 只停掉部署的组件，**不删除** fdb 安装包、`poc_bundle`、`$DEPLOY_DIR` 及数据（下次 `deploy_fdb.sh` 会自行清理数据目录）。

每台机器（root）：

```bash
service foundationdb stop 2>/dev/null || true    # 停 FDB 服务（systemd/init）
pkill -x fdbmonitor 2>/dev/null || true          # 若用 direct 模式启动，直接杀进程
pkill -x fdbserver  2>/dev/null || true
pkill -x datalayers 2>/dev/null || true          # 停 datalayers（集群 + 单机）
```

机器3（跑 MinIO）宿主机：

```bash
docker rm -f dl-minio 2>/dev/null || true
```

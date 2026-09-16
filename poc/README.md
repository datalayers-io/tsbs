# Datalayers TSBS POC

本目录包含了针对 Datalayers 的 TSBS（Time Series Benchmark Suite）基准测试 POC
（Proof of Concept）所需的全部脚本与配置。POC 的目标是：生成 100 万台主机的
`cpu-only` 时间序列数据（fresh）以及其时间戳全部早于 fresh 数据的 10% 数据量
（stale，用于验证乱序/过期数据写入），灌入 Datalayers，然后运行 15 种标准查询。

## 目录结构

```
poc/
├── bench.sh                    # 一键压测入口（读取 bench_config.yaml，探测并执行各步骤）
├── bench_config.yaml           # 压测配置（服务地址/dlsql+dldump 目录/各步骤开关/并发数）
├── build/                      # 编译脚本（生成 bin/ 下的二进制 + 打包）
│   ├── build_local_cgo.sh      #   本地一键 CGO 编译 + 打包
│   └── build_on_kylin.sh       #   麒麟 V10 目标机上编译（换源/装 Go/装依赖/编译/打包）
├── load_config/                # POC 灌数配置（tsbs_load 使用）
│   ├── load_data_poc.yaml      #   灌 fresh 数据（2026-01-01 00:00~12:00）
│   └── load_stale_data_poc.yaml#   灌 stale 数据（2025-12-31 00:00~12:00）
├── sql/                        # POC 用的 SQL 文件
│   ├── create.sql              #   建库建表（CREATE DATABASE + CREATE TABLE benchmark.cpu）
│   ├── alter.sql               #   加列/查询/删列/查询（test_alter.sh 使用）
│   └── sample.sql              #   建 cpu_sample/灌 1000 万行/flush/查 sst_files（压缩率用）
├── scripts/                    # POC 运行脚本
│   ├── gen_data_poc.sh         #   生成 POC 数据（可选 stale 参数）
│   ├── gen_queries_poc.sh      #   生成全部 15 种 POC 查询
│   ├── rewrite_queries_poc.sh  #   按 poc_hints.yaml 重写查询的 SQL hint
│   ├── poc_hints.yaml          #   每个查询的 parallel_degree / skip_rollup 配置
│   ├── load_data_poc.sh        #   灌数据（可选 stale 参数，读 load_config/ 对应 yaml）
│   ├── run_queries_poc.sh      #   运行某一种查询（workers + 查询编号 1~15）
│   ├── run_all_queries_poc.sh  #   跑全部 15 种查询，实时进度 + 汇总表格
│   ├── bench_common.sh        #   公共函数：读 bench_config.yaml + 探测 dlsql/dldump/端口
│   ├── test_alter.sh           #   加列/删列验证（执行 sql/alter.sql）
│   ├── compute_compression_ratio.sh  #   cpu_sample 压缩率计算（SST vs CSV）
│   └── pack.sh                 #   把二进制 + 脚本 + 配置打包成可直接运行的目录
└── README.md                   # 本文档
```

## 各文件说明

### build/

- **build_local_cgo.sh**：在当前机器上以 CGO 一键编译所有 Datalayers 相关二进制
  （`make all`），然后调用 `poc/scripts/pack.sh` 打包。
  - 用法：`./poc/build/build_local_cgo.sh`
  - 可用 `CGO_ENABLED=0` 强制静态编译（跨系统分发用）。
  - 注意：CGO 产物是动态链接的，**在比目标机 glibc 更新的发行版上编译的产物，
    拷到麒麟 V10（glibc 2.28）上会运行失败**（`GLIBC_2.xx not found`）。

- **build_on_kylin.sh**：在裸机麒麟 V10 上完成整套构建，产出可直接在目标机运行的
  二进制并打包。步骤：换 Go 代理源（`GOPROXY=https://goproxy.cn,direct`）→
  安装 Go 1.22.x（缺失时从阿里云镜像下载）→ `go mod download` → `make all` →
  `pack.sh`。
  - 用法：`./poc/build/build_on_kylin.sh`
  - 可用环境变量覆盖：`GO_VERSION`、`GO_PREFIX`、`GO_MIRROR`。
  - 依赖 `gcc`/`glibc-devel`（CGO 编译需要），缺失时脚本会用 dnf/yum 尝试安装。

### load_config/

- **load_data_poc.yaml**：灌 fresh 数据。`file.location` 指向
  `generated_data/datalayers/cpu-only-1000000-20260101-12h.data`，连接
  `localhost:8360`（Datalayers Arrow Flight SQL 端口），写入 `benchmark` 库。
- **load_stale_data_poc.yaml**：灌 stale 数据，指向
  `generated_data/datalayers/cpu-only-100000-20251231-12h.data`。

### scripts/

- **gen_data_poc.sh**：调用 `bin/tsbs_generate_data` 生成 POC 数据。
  - 无参：fresh 数据 —— `--scale=1000000`、`--log-interval=30s`、
    `2026-01-01 00:00:00 ~ 12:00:00`（约 1.44e9 个点）。
  - `stale` 参数：stale 数据 —— 同样的 30s 间隔与 12h 窗口，但时间在
    `2025-12-31 00:00:00 ~ 12:00:00`；`--scale=100000`，约为 fresh 数据量的 10%。
  - 可用环境变量 `POC_SCALE` / `POC_STALE_SCALE` 覆盖主机数（小规模验证时用）。
  - 产物：`generated_data/datalayers/cpu-only-<scale>-<日期>-12h.data`。

- **gen_queries_poc.sh**：自包含的查询生成脚本（不依赖 `my_scripts/generate_query.sh`）。
  为 15 种查询类型逐个调用 `bin/tsbs_generate_queries`，scale=1000000、
  时间范围 2026-01-01 00:00 ~ 12:00（与 fresh 数据对齐），每个类型 `--queries`
  默认 100 条（可用环境变量 `NUM_QUERIES` 覆盖，scale 可用 `POC_SCALE` 覆盖）。
  - 注意：部分查询类型需要 8 台主机（single-groupby-*-8-1、cpu-max-all-8），
    scale 需 ≥ 8，否则生成时会 panic。
  - 产物：`generated_query/datalayers/cpu-only/poc/<查询类型>.query`

- **poc_hints.yaml**：每个查询类型的 SQL hint 配置（`parallel_degree` 与
  `skip_rollup`），`default` 段兜底未列出的类型。初值取自生成器内置的
  "large" 场景值，可自行调优。

- **rewrite_queries_poc.sh**：调用 `bin/rewrite_query_hints_config`，按
  `poc_hints.yaml` 批量重写 `generated_query/datalayers/cpu-only/poc/` 下所有
  `.query` 文件（原地原子替换）。改 hint 只需改 yaml 再跑一次本脚本，无需重新生成查询。

- **load_data_poc.sh**：灌数据。
  - 无参：按 `poc/load_config/load_data_poc.yaml` 灌 fresh 数据。
  - `stale` 参数：按 `poc/load_config/load_stale_data_poc.yaml` 灌 stale 数据。
  - 数据文件路径按 `POC_SCALE`/`POC_STALE_SCALE` 自动推导（与 gen_data_poc.sh 产物一致），
    也可用 `DATA_FILE` 显式指定；`SQL_ENDPOINT` 覆盖 Arrow Flight 地址。

- **run_queries_poc.sh**：运行单个查询。参数：`<workers> <query-number(1~15)>`。
  查询编号与 `my_scripts/run_queries_datalayers.sh` 一致：
  1~6 single-groupby-*、7~8 cpu-max-all-*、9~11 double-groupby-*、
  12~13 high-cpu-*、14 lastpoint、15 groupby-orderby-limit。
  默认连接 `localhost:8360`（可用 `SQL_ENDPOINT` 覆盖）。

- **run_all_queries_poc.sh**：跑全部 15 种查询并汇总。
  - 参数：`<workers> [print-interval=100] [query-dir]`。
  - 逐条实时打印进度（借助 tsbs 自身的 `--print-interval` 进度输出），
    每个查询的完整日志写入 `results/poc-<时间戳>/<查询类型>.log`，
    最后打印汇总表格（query + parallel_degree + skip_rollup + mean(ms) + qps）
    并生成 `summary.tsv`。
  - 可用环境变量：`SQL_ENDPOINT`、`RESULTS_DIR`。

- **pack.sh**：把 `bin/`（5 个二进制）和整个 `poc/`（scripts/load_config/bench.sh/
  bench_config.yaml/create.sql/README）打包到一个自包含目录（默认
  `dist/datalayers-<host>-<时间>`），保持仓库相对布局，拷到目标机后可直接运行。
  POC 流程只依赖 `bin/` 和 `poc/`，因此通用的 `my_scripts/` 与
  `load_config/datalayers/` 不打包。

### 根目录文件

- **bench_config.yaml**：一键压测配置。字段：
  - `flight_addr`：Arrow Flight SQL 地址（灌数/查询/dlsql/dldump 连接用）
  - `http_addr`：Datalayers HTTP 地址（探测用）
  - `dlsql_dir`：dlsql 工具目录（留空则用 PATH 中的 dlsql）
  - `dldump_dir`：dldump 工具目录（留空则用 PATH 中的 dldump）
  - `dlsql_extra_args`：传给 dlsql 的附加参数（如 `-d default`）
  - `dlsql_timeout`：dlsql 执行 SQL 的超时秒数（默认 300，避免 DDL 卡死）
  - `dldump_timeout`：dldump 导出的超时秒数（默认 600）
  - `database`：目标数据库名（默认 benchmark）
  - `create_db_table` / `gen_data` / `gen_queries` / `load_data` / `run_queries`：
    各步骤开关（true/false）
  - `query_workers`：查询并发数（run_all_queries_poc.sh 使用）

- **bench.sh**：一键压测入口。
  - 用法：`./poc/bench.sh [config.yaml] [smoke]`（默认 `./poc/bench_config.yaml`）。
  - 加 `smoke` 进入冒烟模式：强制 `POC_SCALE=1000`、`POC_STALE_SCALE=100`
    （约 fresh 168MB / stale 17MB，1,440,000 + 144,000 行），快速验证全流程。
  - 启动时探测：datalayers HTTP/Flight 端口 TCP 可通、dlsql 可用、5 个 tsbs
    二进制齐全。
  - 按配置依次执行建库建表、生成数据、生成查询、灌数、跑全部查询。
  - 建库建表用 `dlsql --load-file poc/sql/create.sql`，**dlsql 连接的是 Arrow
    Flight SQL 端口（flight_addr），不是 HTTP 端口**。
  - 灌数/查询结果写入 `./results/poc-<时间戳>/`，最后打印 load 指标
    （tsbs 原生只提供 rows/sec / metrics/sec 吞吐，无单条写入延迟）与
    查询汇总表格（由 run_all_queries_poc.sh 输出）。
  - 注：load 与查询的实时进度由 tsbs 自身打印——load 按 `reporting-period`
    输出 rows/s 到 stdout，查询按 `--print-interval` 输出到 stderr，脚本会透传。

### sql/

- **create.sql**：建库建表 SQL。`CREATE DATABASE IF NOT EXISTS benchmark;` +
  `CREATE TABLE IF NOT EXISTS benchmark.cpu (...)`，表结构与 TSBS datalayers
  加载器写入的 cpu 表对齐（ts/标签 STRING/usage_* INT64、时间键 ts、按
  hostname 哈希分片）。由 bench.sh 用 `dlsql --load-file` 执行。

- **alter.sql**：加列/删列验证。顺序执行：`alter table cpu add column tmp` →
  `select hostname, tmp from cpu order by ts desc limit 10` →
  `alter table cpu remove column tmp` →
  `select hostname from cpu order by ts desc limit 10`。表名未加库前缀，
  由 `test_alter.sh` 用 `dlsql -d benchmark --load-file` 执行。

- **sample.sql**：压缩率计算 SQL。建 `cpu_sample` 表（字段与 cpu 一致，
  `PARTITIONS 1`、`memtable_size=8GiB`，可容纳 1000 万行不被动 flush）→
  `INSERT INTO cpu_sample SELECT * FROM cpu ORDER BY ts LIMIT 10000000` →
  `FLUSH TABLE cpu_sample SYNC` → 查询 `information_schema.sst_files` 的
  `table`/`file_size`。由 `compute_compression_ratio.sh` 执行。

### scripts/（新增）

- **bench_common.sh**：公共函数库。`load_bench_config` 读取 bench_config.yaml，
  `probe_bench_env` 探测 datalayers HTTP/Flight 端口、dlsql、dldump。
  被 test_alter.sh / compute_compression_ratio.sh source。

- **test_alter.sh**：验证加列/删列。用 `dlsql -d <database> --load-file`
  顺序执行 `sql/alter.sql`。用法：`./poc/scripts/test_alter.sh [config.yaml]`。

- **compute_compression_ratio.sh**：计算 cpu_sample 的压缩率。
  - 流程：探测 → `drop cpu_sample`（幂等）→ 执行 `sql/sample.sql` →
    从 sst_files 输出求和 `file_size`（datalayers 数据大小）→ 用
    `dldump -f csv` 导出 cpu_sample 到 CSV → 计算 csv 大小 → 打印三组比值：
    1) datalayers 是 csv 的百分之多少；2) csv 是 datalayers 的多少倍；
    3) datalayers:csv（datalayers 取 1）。
  - 依赖 `dldump`（`-h/-P/-u/-p/-d/-t/-o/-f csv`，输出 `<output>/<db>_<table>.csv`）。
  - 用法：`./poc/scripts/compute_compression_ratio.sh [config.yaml]`。
  - 输出：`./results/compression-<时间戳>/`。

## 依赖的二进制

运行前需先在 `bin/` 下编译好（`make all`，5 个）：
`tsbs_generate_data`、`tsbs_generate_queries`、`tsbs_load`、
`tsbs_run_queries_datalayers`、`rewrite_query_hints_config`。

在麒麟 V10 目标机上直接用 `poc/build/build_on_kylin.sh` 完成编译 + 打包；
在本地则用 `poc/build/build_local_cgo.sh`。

## 完整使用流程

```bash
# 0. 一键压测（推荐）：先改好 poc/bench_config.yaml，然后
./poc/bench.sh
#    小规模冒烟验证（scale=1000，约 168MB）
./poc/bench.sh smoke

# 或分步执行：
# 1. 编译（本地）
./poc/build/build_local_cgo.sh
#    或目标机
./poc/build/build_on_kylin.sh

# 2. 建库建表（bench.sh 的 create_db_table 步骤，等价于）
dlsql -h <host> -P <http_port> --load-file poc/sql/create.sql

# 3. 生成数据（fresh；加 stale 生成过期数据）
./poc/scripts/gen_data_poc.sh
./poc/scripts/gen_data_poc.sh stale

# 4. 生成查询
./poc/scripts/gen_queries_poc.sh

# 5. （可选）调优 SQL hint：改 poc_hints.yaml 后重写
./poc/scripts/rewrite_queries_poc.sh

# 6. 灌数据（fresh；加 stale 灌过期数据）
./poc/scripts/load_data_poc.sh
./poc/scripts/load_data_poc.sh stale

# 7. 跑单个查询（workers + 查询编号）
./poc/scripts/run_queries_poc.sh 64 14

# 8. 跑全部 15 种查询并汇总
./poc/scripts/run_all_queries_poc.sh 64

# 9. 加列/删列验证
./poc/scripts/test_alter.sh

# 10. 压缩率计算（cpu_sample：SST vs CSV）
./poc/scripts/compute_compression_ratio.sh
```

## 注意事项

- **数据量**：fresh 数据约 1.44e9 个点（约 170GB+），stale 约 1.44e8 个点
  （约 17GB），生成前请确认磁盘空间。
- **hostname 重叠**：TSBS 的 hostname 为 `host_0 ~ host_<scale-1>`。stale 的
  10 万台主机（`host_0 ~ host_99999`）是 fresh 的 10 万台主机的子集，即 stale
  是"同一批主机更早时间"的数据。
- **时间范围**：15 种查询中有 7 种（single-groupby-*-1-12、double-groupby-*、
  high-cpu-*）的时间条件恰好覆盖整个 12h 数据集；`lastpoint` 无时间谓词，
  `groupby-orderby-limit` 无时间下界——这几种在混入 stale 数据时，结果的正确性
  依赖引擎对"取最新/排序截断"的处理。
- **并发度 hint**：生成器会内置一个默认 `parallel_degree`；通过
  `poc_hints.yaml` + `rewrite_queries_poc.sh` 可按查询类型覆盖并附加
  `skip_rollup`，无需重新生成查询。

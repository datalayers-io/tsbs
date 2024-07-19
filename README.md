该仓库是 Datalayers TSBS 项目，修改自 `timescale/tsbs` 项目。
---

# 编译
执行 `make` 编译所有必需的 binaries。编译好的 binaries 会存放在项目根目录的 `bin` 目录下。

# 生成压测数据
使用 TSBS 的模拟算法，生成不同模拟场景的压测数据。这些压测数据会用于写入压测，以及查询压测。
TSBS 提供统一的模拟算法，但是所生成的数据如何序列化，需要每个数据库自己定义。Datalayers 实现了自定义的序列化算法，具体请察看源码 `pkg/targets/datalayers/serializer.go`。

TSBS 原生支持 5 个场景：
- devops：模拟监控多个服务器（host）的场景。每个 host 有 cpu、memory、disk 等最多 9 个监控指标。每个 host 通过 hostname、region、rack 等 tags 标识。
- cpu-only：这是 devops 场景的子集。每个 host 只有 cpu 一个监控指标。
- cpu-single：这是 devops 场景的一个更小的子集。我们只监控一个 host，且只监控这个 host 的一个 cpu。
- devops-generic：这是 devops 场景的超集。它额外提供一个 `max-metrics` 参数，用于控制每个 host 的监控指标的数量。这个场景可以用来测试高基数（high cardinality）场景的性能。
- iot：模拟监控一个卡车公司的多台卡车的场景。每个卡车有一系列监控指标。与 devops 场景最大的区别是，由于现实中卡车可能离线，因此这个场景所生成的数据可能有乱序、丢失、空、突然大量数据等特征。

为了与 TDEngine 等数据库对比，我们计划支持 cpu-only、devops、iot 三个场景。

我们目前只支持 cpu-only、devops 两个场景，这是因为我们还无法处理 iot 场景存在的乱序数据。

以下是生成各个场景的命令：

## cpu-only
执行以下命令，会在项目根目录生成一个名为 `datalayers_cpu-only.data` 的文件。
``` shell
./bin/tsbs_generate_data \
    --use-case="cpu-only" \
    --seed=42 \
    --scale=100 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-01T06:00:00Z" \
    --format="datalayers" \
    --file="./datalayers_cpu-only.data"
```

参数说明：
- use-case：模拟场景，从 `cpu-only`、`devops`、`iot` 三者中选择一个。
- seed：用于模拟算法。当我们对比多个数据库在同一个场景的性能时，我们应该在每次比较时，将 seed 设置为统一的 seed。
- scale：设置不同的 host 的数量。两个 host，如果它们的 tag 不一样，那么就认为它们是不同的 host。
- timestamp-start：从这个时间点开始生成模拟数据。时间是 UTC 时区。
- timestamp-end：在这个时间点结束生成模拟数据。时间是 UTC 时区。
- file：生成数据的存放路径与名称。

## devops
``` shell
./bin/tsbs_generate_data \
    --use-case="devops" \
    --seed=42 \
    --scale=100 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-01T06:00:00Z" \
    --format="datalayers" \
    --file="./datalayers_devops.data"
```

## iot
TODO


# 写入压测
## 配置与写入有关的参数
以下是 `cpu-only` 场景所使用的参数。实际上这里只是所有参数的子集，没有显式指定的参数都使用了 TSBS 提供的默认值。
``` yaml
data-source:
  type: FILE
  file:
    location: ./datalayers_cpu-only.data
loader:
  db-specific:
    sql-endpoint: 127.0.0.1:8360
    batch-size: "5000"
    partition-num: 16
    partition-by-fields:
        - "cpu:hostname,region,datacenter,rack"
  runner:
    batch-size: "5000"
    channel-capacity: "0"
    db-name: benchmark
    do-abort-on-exist: false
    do-create-db: true
    do-load: true
    flow-control: true
    hash-workers: true
    insert-intervals: ""
    limit: "0"
    reporting-period: 5s
    seed: 42
    workers: "8"
  target: datalayers
```

关于 `data-source`、`loader.runner`、`loader.target` 中的参数的含义，可以执行命令 `./bin/tsbs_load load --help` 来察看。

关于 `loader.db-specific` 中的参数的含义：
- sql-endpoint：Datalayers 服务端 Arrow FlightSql 服务的监听端口。
- batch-size：在写入链路上，存在一个用于攒批的 buffer。它的 max size 由 `loader.runner.batch-size` 来决定。但是创建这个 buffer 时，TSBS 默认创建一个 capacity = 0 的 buffer。另一方面，我们在创建 buffer 时，由于 TSBS 框架的限制，我们无法获取到 `loader.runner.batch-size` 这个参数。因此我们加入了这个参数，使得创建 buffer 时所设置的 capacity 与 max size 等同。以减少动态 re-allocate 带来的开销。你应该将 `loader.db-specific.batch-size` 与 `loader.runner.batch-size` 设置为一致的值。
- partition-num：每个表的 partition 数量。
- partition-by-fields：为每个表指定建表时使用的 `PARTITION BY` 列。这个参数接受一个 array of strings。每个 string 符合这样的格式：`<table name>:<field name>,<field name>,...`。 `table_name` 表示为哪个表指定 partition by 列，冒号后面的，是用逗号隔开的各个列的列名。

### 执行写入
执行以下命令以针对 `cpu-only` 场景进行写入压测。
``` shell
./bin/tsbs_load load datalayers --config=./load_config/cpu-only.yaml
```
你可以参考这个命令，来针对其他场景进行写入压测。

# 查询压测
TODO
## 生成查询命令
## 执行查询

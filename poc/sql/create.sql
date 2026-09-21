-- Datalayers POC 建库建表脚本。
-- 由 bench.sh 在 create_db_table: true 时通过 dlsql --load-file 执行。
--
-- 表结构对齐 TSBS datalayers 加载器写入的 cpu 表：
--   列类型与 pkg/targets/datalayers/client/client.go 的
--   arrowDataTypeToDatalayersDataType 一致（TIMESTAMP(9)/STRING/INT64），
--   以 ts 为时间键，按 hostname 哈希分片。
--
-- 表选项（POC 场景约定）：
--   - PARTITIONS 12：分区数对应 load 并行 worker 数（12），避免写热点；
--   - UPDATE_MODE=APPEND：时序日志只追加、不去重（double-groupby 等聚合走 rollup）；
--   - COMPACT_MODE=TIME_BUCKET：只做按时间窗口分桶的 compaction，关闭按大小/重叠的
--     默认 compaction 与定时触发，避免查询期间抖动；
--   - COMPACT_WINDOW=1h：分桶窗口（同时决定 flush 的 ascending 切窗），配合 rollup 的
--     1h 粒度，使按时间范围过滤的查询能有效裁剪 SST 文件。

CREATE DATABASE IF NOT EXISTS benchmark;

CREATE TABLE IF NOT EXISTS benchmark.cpu (
    ts TIMESTAMP(9) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    hostname STRING NOT NULL,
    region STRING,
    datacenter STRING,
    rack STRING,
    os STRING,
    arch STRING,
    team STRING,
    service STRING,
    service_version STRING,
    service_environment STRING,
    usage_user INT64,
    usage_system INT64,
    usage_idle INT64,
    usage_nice INT64,
    usage_iowait INT64,
    usage_irq INT64,
    usage_softirq INT64,
    usage_steal INT64,
    usage_guest INT64,
    usage_guest_nice INT64,
    timestamp key(ts)
)
PARTITION BY HASH(hostname) PARTITIONS 12
ENGINE=TimeSeries
with(
    MEMTABLE_SIZE=2GB,
    UPDATE_MODE=APPEND,
    COMPACT_MODE=TIME_BUCKET,
    COMPACT_WINDOW=1h,
    STORAGE_TYPE=S3
);

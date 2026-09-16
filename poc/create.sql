-- Datalayers POC 建库建表脚本。
-- 由 bench.sh 在 create_db_table: true 时通过 dlsql --load-file 执行。
--
-- 表结构对齐 TSBS datalayers 加载器写入的 cpu 表：
--   列类型与 pkg/targets/datalayers/client/client.go 的
--   arrowDataTypeToDatalayersDataType 一致（TIMESTAMP(9)/STRING/INT64），
--   以 ts 为时间键，按 hostname 哈希分片。

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
PARTITION BY HASH(hostname) PARTITIONS 64
ENGINE=TimeSeries
with(memtable_size=2048MiB);

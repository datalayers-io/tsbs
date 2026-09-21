-- 稳定性测试专用建表脚本（与 sql/create.sql 的 cpu 表结构一致）。
--
-- 设计：稳定性测试**写入独立的库**（benchmark_stable.cpu），**查询仍在 benchmark.cpu**，
-- 这样持续写入不会影响查询所依赖的数据集，读写互不干扰。
--
-- 由稳定性测试流程用 dlsql --load-file 执行一次（见 poc/scripts/stability_24h.sh）。
--   dlsql -h <flight_host> -P <flight_port> -u admin -p public --load-file poc/sql/create_stable.sql

CREATE DATABASE IF NOT EXISTS benchmark_stable;

CREATE TABLE IF NOT EXISTS benchmark_stable.cpu (
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
PARTITION BY HASH(hostname) PARTITIONS 6
ENGINE=TimeSeries
with(
    MEMTABLE_SIZE=2GB,
    UPDATE_MODE=APPEND,
    COMPACT_MODE=DISABLE
);

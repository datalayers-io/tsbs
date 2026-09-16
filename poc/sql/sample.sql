-- Datalayers POC 压缩率计算 SQL（由 poc/scripts/compute_compression_ratio.sh 用 dlsql --load-file 执行）。
-- 步骤：
--   1. 建 cpu_sample 表，字段与 cpu 完全一致，但 PARTITIONS=1、memtable_size 足够大
--      （8GiB，能容纳 1000 万行而不被动 flush）、COMPACT_WINDOW=365d（跨天的数据
--      也落在同一个时间窗口内，flush 后只产生一个 SST 文件）；
--   2. 从 cpu 取 1000 万行写入 cpu_sample；
--   3. flush table cpu_sample sync 将数据刷到 SST；
--   4. 查询 information_schema.sst_files 得到 cpu_sample 所有文件的 file_size，
--      由脚本求和得到 datalayers 数据文件总大小。
--
-- 注意：执行前脚本会先 drop cpu_sample，保证幂等。

CREATE TABLE IF NOT EXISTS benchmark.cpu_sample (
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
PARTITION BY HASH(hostname) PARTITIONS 1
ENGINE=TimeSeries
with(memtable_size=8GiB, COMPACT_WINDOW=365d);

INSERT INTO benchmark.cpu_sample SELECT * FROM benchmark.cpu ORDER BY ts LIMIT 10000000;

FLUSH TABLE benchmark.cpu_sample SYNC;

SELECT table, file_size FROM information_schema.sst_files WHERE table = 'cpu_sample';

-- Datalayers POC 压缩率计算 SQL（由 poc/scripts/compute_compression_ratio.sh 用 dlsql 逐条执行）。
-- 步骤：
--   1. 建 cpu_sample 表，字段与 cpu 完全一致；PARTITIONS=4、memtable_size=2GB、
--      STORAGE_TYPE=local、COMPACT_WINDOW=365d（跨天的数据落在同一时间窗口，
--      flush 后每个分区各产生一个 SST 文件）；
--   2. 从 cpu 取 1000 万行写入 cpu_sample；
--   3. flush table cpu_sample sync 将数据刷到 SST；
--   4. 查询 information_schema.sst_files，用 SQL 的 SUM(file_size) 直接求出
--      cpu_sample 所有文件的总大小（datalayers 数据文件大小），脚本只解析该值。
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
PARTITION BY HASH(hostname) PARTITIONS 4
ENGINE=TimeSeries
with(memtable_size=2GB, COMPACT_WINDOW=365d, STORAGE_TYPE=local);

INSERT INTO benchmark.cpu_sample SELECT * FROM benchmark.cpu ORDER BY ts LIMIT 10000000;

FLUSH TABLE benchmark.cpu_sample SYNC;

SELECT SUM(file_size) AS total_file_size FROM information_schema.sst_files WHERE `table` = 'cpu_sample';

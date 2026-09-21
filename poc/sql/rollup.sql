-- Datalayers POC 创建 cpu 表的 1h 窗口 rollup（由 poc/bench.sh 在 create_rollup: true 时执行）。
-- 必须在 load fresh + stale 数据完成后执行；GROUP BY (hostname) 为 cpu 的 entity/partition key。
-- 指标列聚合为 <col>_count/_sum/_min/_max；INTERVAL 1h 即 1 小时时间窗口。
-- 先 FLUSH 源表，确保初始物化覆盖全部数据（否则由 realtime aggregation 补齐 delta）。

FLUSH TABLE benchmark.cpu SYNC;

DROP ROLLUP IF EXISTS cpu_rollup_1h;

CREATE ROLLUP cpu_rollup_1h ON cpu (
    usage_user, usage_system, usage_idle, usage_nice, usage_iowait,
    usage_irq, usage_softirq, usage_steal, usage_guest, usage_guest_nice
)
GROUP BY (hostname)
INTERVAL 1h;

-- ============================================================================
-- 说明：rollup 表的表选项现在由引擎在 CREATE ROLLUP 时直接设置，无需在 POC
-- 脚本里 ALTER：
--   * COMPACT_MODE=TIME_BUCKET（只做按时间窗口分桶的 compaction，禁用周期/按大小触发）
--   * COMPACT_WINDOW=1d（按 rollup 粒度选定的分桶窗口，见下）
--   * MEMTABLE_SIZE=2GB（保证一次性物化 12M 行不会因默认 256MB memtable 背压卡住）
--
-- 为什么需要 TIME_BUCKET + 合理窗口：rollup 路由只服务 date_trunc 粒度与 rollup
-- interval 相等的查询。POC 里只有两类：
--   * double-groupby-*：窗口固定为整个数据集（12h），任何窗口都无法裁剪，只需文件少；
--   * cpu-max-all-*：随机 8h 窗口，若 rollup 文件按 1h 对齐可裁掉区间外的小时。
-- 综合考虑取 1d（小时粒度的 24 倍）：文件数远少于 1h 窗口，同时仍支持按窗口裁剪；
-- 若未来出现大量短区间、按小时对齐的 rollup 查询，可在引擎中把窗口调细。
-- ============================================================================

-- 确保物化数据已落盘，再按窗口做分桶压实（把物化产生的 delta 并进窗口文件）。
-- COMPACT 是异步的，bench.sh 会在执行完本文件后轮询等待其收敛。
FLUSH TABLE benchmark.cpu_rollup_1h SYNC;

COMPACT TABLE benchmark.cpu_rollup_1h;

-- 首轮若存在“单个未对齐文件”的窗口会跳过，需要再跑一轮收敛；两轮都提交，
-- 后台会自动串行执行，不会重复压缩已对齐的窗口。
COMPACT TABLE benchmark.cpu_rollup_1h;

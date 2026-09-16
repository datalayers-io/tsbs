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

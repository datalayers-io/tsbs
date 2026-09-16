-- Datalayers POC 表结构变更验证 SQL（由 poc/scripts/test_alter.sh 用 dlsql --load-file 顺序执行）。
-- 依次：加列 -> 查询新列 -> 删列 -> 查询（确认删列后仍正常）。
-- 注意：Datalayers 语法为 ADD COLUMN <col> <type> 与 DROP COLUMN（非 REMOVE）。
-- 表名未加库前缀，dlsql 需带 -d benchmark 指定数据库。

ALTER TABLE cpu ADD COLUMN tmp INT64;

SELECT hostname, tmp FROM cpu ORDER BY ts DESC LIMIT 10;

ALTER TABLE cpu DROP COLUMN tmp;

SELECT hostname FROM cpu ORDER BY ts DESC LIMIT 10;

-- TSALIGN 002: 结果版本化
-- 每次回填执行产出一个独立的 alignment_run 版本：
--   * alignment_run.job_id 去掉唯一约束（一任务多版本），加 (job_id, version_no) 唯一约束
--   * request_key 幂等键：任务参数 + 输入数据指纹，重复执行同一回填请求只生效一次
--   * input_watermark 冻结输入证据；published_at 标记成功发布的版本
--   * alignment_job.current_run_id 指向最近一次成功发布的版本（失败版本不占用）
-- 既有数据：每个任务的既有 run 记为 v1，completed 的补 published_at，
-- request_key 无法回溯指纹，以 legacy-<id> 占位保证唯一；current_run_id 指向最新 completed 版本。

ALTER TABLE alignment_run DROP CONSTRAINT IF EXISTS alignment_run_job_id_key;

ALTER TABLE alignment_run ADD COLUMN IF NOT EXISTS version_no INTEGER;
ALTER TABLE alignment_run ADD COLUMN IF NOT EXISTS request_key VARCHAR(64);
ALTER TABLE alignment_run ADD COLUMN IF NOT EXISTS input_watermark JSON DEFAULT '[]';
ALTER TABLE alignment_run ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;

UPDATE alignment_run SET version_no = 1 WHERE version_no IS NULL;
UPDATE alignment_run SET published_at = finished_at WHERE status = 'completed' AND published_at IS NULL;
UPDATE alignment_run SET request_key = 'legacy-' || id WHERE request_key IS NULL;

ALTER TABLE alignment_run ALTER COLUMN version_no SET NOT NULL;
ALTER TABLE alignment_run ALTER COLUMN request_key SET NOT NULL;

ALTER TABLE alignment_run
    ADD CONSTRAINT uq_alignment_run_job_version UNIQUE (job_id, version_no);
CREATE UNIQUE INDEX IF NOT EXISTS uq_alignment_run_request_key ON alignment_run (request_key);
CREATE INDEX IF NOT EXISTS ix_alignment_run_job_id ON alignment_run (job_id);

ALTER TABLE alignment_job ADD COLUMN IF NOT EXISTS current_run_id INTEGER REFERENCES alignment_run(id);

UPDATE alignment_job j SET current_run_id = (
    SELECT r.id FROM alignment_run r
    WHERE r.job_id = j.id AND r.status = 'completed'
    ORDER BY r.id DESC
    LIMIT 1
) WHERE j.current_run_id IS NULL;

# Schema baseline

Runtime schema is created by SQLAlchemy `Base.metadata.create_all()` on backend startup
(see `src/config/database.py`), which keeps the 5-service demo stack one-command startable.
`versions/001_initial.sql` is the reviewable baseline DDL kept in sync with the models for
teams that prefer explicit migrations / Alembic bootstrapping.

`versions/002_versioned_runs.sql` upgrades pre-versioning databases in place: each backfill
execution becomes its own immutable `alignment_run` version (`version_no` + idempotent
`request_key` + frozen `input_watermark`), and `alignment_job.current_run_id` points at the
latest successfully published version so failed reruns never clobber the online result.

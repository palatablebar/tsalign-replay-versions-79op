"""Execute one alignment job end-to-end: raw points -> align engine -> gap engine -> DB.

版本化语义（每次执行 = 一个独立的 AlignmentRun 版本）：

- 版本由 ``request_key``（任务参数 + 输入数据指纹）标识：同一回填请求重复执行
  只生效一次，命中已存在的版本直接返回，不产生新版本；
- 成功版本发布：写快照、新建该版本的缺口事件、``published_at`` 落时间，
  并把 ``job.current_run_id`` 指向它 —— 任务的"当前结果"只随成功移动；
- 失败版本保留原因（failure_reason / failure_detail）供排查，但既不发布，
  也不触碰任何历史版本及其缺口标注 —— 在线结果不会被失败重跑打坏；
- 历史版本与其缺口事件永不删除，缺口标注只属于产生它的版本。
"""
from __future__ import annotations

import time
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.constants.enums import JobStatus, RunStatus
from src.models import AlignmentJob, AlignmentRun, GapEvent, RawTelemetry, Signal
from src.services import gap_engine, versioning
from src.services.align_engine import SignalInput, align_job
from src.utils.logger import log_event
from src.utils.timeutil import from_epoch, iso, to_epoch


def _load(inputs: list[Signal], db: Session, window_start: float, window_end: float) -> list[SignalInput]:
    payloads: list[SignalInput] = []
    for signal in inputs:
        rows = db.execute(
            select(RawTelemetry.ts, RawTelemetry.value, RawTelemetry.seq)
            .where(
                RawTelemetry.signal_id == signal.id,
                RawTelemetry.ts >= window_start,
                RawTelemetry.ts <= window_end,
            )
            .order_by(RawTelemetry.ts)
        ).all()
        payloads.append(
            SignalInput(
                signal_id=signal.id,
                signal_code=signal.code,
                device_code=signal.device_code,
                device_name=signal.device_name,
                measure_name=signal.measure_name,
                unit=signal.unit,
                source_id=signal.source_id,
                source_code=signal.source.code if signal.source else "",
                points=[(float(row[0]), float(row[1]), int(row[2])) for row in rows],
            )
        )
    return payloads


def _sync_job_with_run(db: Session, job: AlignmentJob, run: AlignmentRun) -> None:
    """幂等命中后把任务状态纠正到与已生效版本一致（dispatch 可能刚把它重置成 pending）。"""
    if run.status == RunStatus.COMPLETED.value:
        job.status = JobStatus.COMPLETED.value
        job.error_reason = ""
        if not job.current_run_id:
            job.current_run_id = run.id
    elif run.status == RunStatus.FAILED.value:
        job.status = JobStatus.FAILED.value
        job.error_reason = run.failure_reason or ""
    else:
        job.status = JobStatus.RUNNING.value
    db.commit()


def _next_version_no(db: Session, job_id: int) -> int:
    current = db.scalar(
        select(func.coalesce(func.max(AlignmentRun.version_no), 0)).where(AlignmentRun.job_id == job_id)
    )
    return int(current or 0) + 1


def execute_job(db: Session, job: AlignmentJob) -> AlignmentRun:
    started = time.perf_counter()
    signals = list(
        db.scalars(select(Signal).where(Signal.code.in_(job.signal_codes or [])).order_by(Signal.id))
    )
    window_start = to_epoch(job.window_start)
    window_end = to_epoch(job.window_end)
    inputs = _load(signals, db, window_start, window_end)
    watermark = versioning.build_watermark([(item.signal_code, item.points) for item in inputs])
    request_key = versioning.request_key(job, watermark)

    # 幂等：同一回填请求（参数与输入数据都未变）只生效一次
    existing = db.scalar(select(AlignmentRun).where(AlignmentRun.request_key == request_key))
    if existing is not None:
        _sync_job_with_run(db, job, existing)
        log_event(
            "ALIGNMENT_DEDUP_HIT",
            job_id=job.id,
            run_id=existing.id,
            version=existing.version_no,
            status=existing.status,
        )
        return existing

    # 每次回填形成独立版本；并发执行同一请求由 request_key 唯一约束兜底
    run: AlignmentRun | None = None
    for attempt in (1, 2):
        run = AlignmentRun(
            job_id=job.id,
            version_no=_next_version_no(db, job.id),
            request_key=request_key,
            input_watermark=watermark,
            status=RunStatus.RUNNING.value,
        )
        db.add(run)
        job.status = JobStatus.RUNNING.value
        job.error_reason = ""
        try:
            db.commit()
            break
        except IntegrityError:
            db.rollback()
            duplicated = db.scalar(select(AlignmentRun).where(AlignmentRun.request_key == request_key))
            if duplicated is not None:
                _sync_job_with_run(db, job, duplicated)
                log_event("ALIGNMENT_DEDUP_RACE", job_id=job.id, run_id=duplicated.id)
                return duplicated
            if attempt == 2:  # version_no 并发冲突已重试一次
                raise
    db.refresh(run)

    aligned = align_job(
        inputs,
        window_start=window_start,
        window_end=window_end,
        grid_s=int(job.grid_s),
        agg=job.agg_strategy,
    )
    result = aligned.to_dict()

    if aligned.failure:
        return _finalize_failure(db, job, run, aligned.failure, aligned.failure_detail, result, started)

    gaps = gap_engine.analyze(result, job.fill_strategy)
    if gaps.get("failure"):
        return _finalize_failure(db, job, run, gaps["failure"], {"reason": "对齐后无任何观测值"}, result, started)

    components = gaps["components"]
    drifts = result.get("drift") or []
    max_drift = max(drifts, key=lambda item: abs(item["drift_ms"]), default={"drift_ms": 0.0})
    result["components"] = components
    result["series"] = gaps["series"]
    result["gaps"] = gaps["gaps"]
    result["window_start"] = iso(job.window_start)
    result["window_end"] = iso(job.window_end)
    result["agg_strategy"] = job.agg_strategy
    result["fill_strategy"] = job.fill_strategy

    run.status = RunStatus.COMPLETED.value
    run.duration_ms = int((time.perf_counter() - started) * 1000)
    run.finished_at = datetime.now()
    run.published_at = run.finished_at
    run.coverage_pct = round(components["coverage"] * 100, 3)
    run.drift_ms = float(max_drift["drift_ms"])
    run.drift_confidence = float(result.get("drift_confidence") or 0.0)
    run.anomaly_rate = round(components["anomaly_rate"] * 100, 3)
    run.quality_score = float(components["quality_score"])
    run.gap_count = int(components["gap_count"])
    run.gap_total_s = float(components["gap_total_s"])
    run.point_count = int(result["point_count"])
    run.bucket_count = int(result["bucket_count"])
    run.failure_reason = ""
    run.result_json = result

    # 缺口事件只属于本版本：只插入，绝不删除历史版本的记录与标注
    signal_by_code = {signal.code: signal for signal in signals}
    for gap in gaps["gaps"]:
        signal = signal_by_code.get(gap["signal_code"])
        if signal is None:
            continue
        db.add(
            GapEvent(
                run_id=run.id,
                signal_id=signal.id,
                start_ts=from_epoch(float(gap["start_ts"])),
                end_ts=from_epoch(float(gap["end_ts"])),
                duration_s=float(gap["duration_s"]),
                bucket_length=int(gap["bucket_length"]),
                fill_strategy=gap["fill_strategy"],
                confidence=float(gap["confidence"]),
                severity=gap["severity"],
            )
        )
    # 任务指向最近一次成功发布的版本
    job.current_run_id = run.id
    job.status = JobStatus.COMPLETED.value
    job.error_reason = ""
    db.commit()
    db.refresh(run)
    log_event(
        "ALIGNMENT_FINISHED",
        job_id=job.id,
        run_id=run.id,
        version=run.version_no,
        coverage=run.coverage_pct,
        drift_ms=run.drift_ms,
        quality=run.quality_score,
        gaps=run.gap_count,
    )
    return run


def _finalize_failure(
    db: Session,
    job: AlignmentJob,
    run: AlignmentRun,
    reason: str,
    detail: dict,
    result: dict,
    started: float,
) -> AlignmentRun:
    """失败版本留痕：保留原因，但不发布、不替换当前结果、不动历史缺口标注。"""
    run.status = RunStatus.FAILED.value
    run.duration_ms = int((time.perf_counter() - started) * 1000)
    run.finished_at = datetime.now()
    run.failure_reason = reason
    run.result_json = {
        **result,
        "components": {},
        "series": [],
        "gaps": [],
        "failure_detail": detail,
    }
    job.status = JobStatus.FAILED.value
    job.error_reason = reason
    db.commit()
    db.refresh(run)
    log_event("ALIGNMENT_FAILED", job_id=job.id, run_id=run.id, version=run.version_no, reason=reason, detail=detail)
    return run

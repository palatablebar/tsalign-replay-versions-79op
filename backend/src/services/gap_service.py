"""Gap-event queries and analyst annotations.

缺口事件（连同标注）只属于产生它的结果版本：重跑只新增版本的缺口行，
历史版本的一行不删。列表默认只看各任务"当前发布版本"的缺口；
传 run_id 可精确追溯某个历史版本。
"""
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.constants.error_codes import AppError, ErrorCode
from src.models import AlignmentJob, AlignmentRun, GapEvent
from src.utils.timeutil import iso


def _to_dict(gap: GapEvent, run: AlignmentRun | None) -> dict:
    signal = gap.signal
    return {
        "id": gap.id,
        "run_id": gap.run_id,
        "run_name": run.job.name if run and run.job else "",
        "version_no": run.version_no if run else None,
        "run_status": run.status if run else "",
        "signal_code": signal.code if signal else "",
        "device_code": signal.device_code if signal else "",
        "device_name": signal.device_name if signal else "",
        "measure_name": signal.measure_name if signal else "",
        "unit": signal.unit if signal else "",
        "start_ts": iso(gap.start_ts),
        "end_ts": iso(gap.end_ts),
        "duration_s": gap.duration_s,
        "bucket_length": gap.bucket_length,
        "fill_strategy": gap.fill_strategy,
        "confidence": gap.confidence,
        "severity": gap.severity,
        "annotated": gap.annotated,
        "annotation": gap.annotation or "",
    }


def list_gaps(
    db: Session,
    run_id: int | None = None,
    severity: str | None = None,
    device_code: str | None = None,
    current_only: bool = True,
) -> list[dict]:
    statement = select(GapEvent).order_by(GapEvent.start_ts, GapEvent.id)
    if run_id:
        # 指定版本追溯：该版本的缺口（含标注）原样返回
        statement = statement.where(GapEvent.run_id == run_id)
    elif current_only:
        # 默认口径：只列各任务当前发布版本的缺口，历史版本不重复计数
        statement = statement.join(AlignmentRun, GapEvent.run_id == AlignmentRun.id).join(
            AlignmentJob, AlignmentJob.current_run_id == AlignmentRun.id
        )
    if severity:
        statement = statement.where(GapEvent.severity == severity)
    gaps = list(db.scalars(statement))
    runs = {run.id: run for run in db.scalars(select(AlignmentRun))}
    items = [_to_dict(gap, runs.get(gap.run_id)) for gap in gaps]
    if device_code:
        items = [item for item in items if item["device_code"] == device_code]
    return items


def annotate(db: Session, gap_id: int, text: str) -> GapEvent:
    gap = db.get(GapEvent, gap_id)
    if gap is None:
        raise AppError(ErrorCode.NOT_FOUND, "缺口事件不存在", status_code=404)
    gap.annotation = text.strip()
    gap.annotated = True
    db.commit()
    db.refresh(gap)
    return gap

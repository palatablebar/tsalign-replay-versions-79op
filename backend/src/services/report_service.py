"""Quality report aggregates for the /reports page.

报告页是"在线质量"投影：趋势、均值、漂移台账与缺口统计都只基于各任务
当前成功发布的版本；历史版本与失败版本保留在运行列表里追溯，不参与汇总，
失败重跑因此不会打坏报告口径。
"""
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.constants.enums import JobStatus
from src.constants.messages import CUSTOMER
from src.models import AlignmentJob, AlignmentRun, GapEvent, User
from src.services import auth_service, run_service
from src.services.gap_engine import QUALITY_FORMULA
from src.utils.timeutil import iso, now_local


def summary(db: Session, user: User, daily_limit: int) -> dict:
    runs = list(db.scalars(select(AlignmentRun).order_by(AlignmentRun.id.desc())))
    current_ids = {
        int(row[0])
        for row in db.execute(
            select(AlignmentJob.current_run_id).where(AlignmentJob.current_run_id.is_not(None))
        ).all()
    }
    # 当前发布版本集合（每任务至多一个，且必为 completed）
    published = [run for run in runs if run.id in current_ids]
    quality_values = [run.quality_score for run in published if run.quality_score]
    coverage_values = [run.coverage_pct for run in published if run.coverage_pct]
    if current_ids:
        gap_total = int(
            db.scalar(select(func.count(GapEvent.id)).where(GapEvent.run_id.in_(current_ids))) or 0
        )
        severity_rows = db.execute(
            select(GapEvent.severity, func.count(GapEvent.id))
            .where(GapEvent.run_id.in_(current_ids))
            .group_by(GapEvent.severity)
        ).all()
    else:
        gap_total = 0
        severity_rows = []
    severity_map = {str(row[0]): int(row[1]) for row in severity_rows}
    drift_table = []
    for run in published[:8]:
        result = run.result_json or {}
        for item in result.get("drift") or []:
            drift_table.append(
                {
                    "run_id": run.id,
                    "job_name": run.job.name if run.job else "",
                    "source_code": item.get("source_code", ""),
                    "drift_ms": item.get("drift_ms", 0),
                    "confidence": item.get("confidence", 0),
                }
            )
    trend = [
        {
            "run_id": run.id,
            "job_name": run.job.name if run.job else "",
            "quality_score": run.quality_score,
            "coverage_pct": run.coverage_pct,
            "finished_at": iso(run.finished_at),
        }
        for run in reversed(published[:10])
    ]
    return {
        "customer": CUSTOMER,
        "generated_at": iso(now_local()),
        "run_count": len(runs),
        "completed_runs": len(published),
        "average_quality": round(sum(quality_values) / len(quality_values), 2) if quality_values else 0.0,
        "best_coverage": round(max(coverage_values), 3) if coverage_values else 0.0,
        "total_gaps": gap_total,
        "formula": QUALITY_FORMULA,
        "quality_trend": trend,
        "drift_table": drift_table,
        "gap_severity": [
            {"severity": "high", "count": severity_map.get("high", 0)},
            {"severity": "medium", "count": severity_map.get("medium", 0)},
            {"severity": "low", "count": severity_map.get("low", 0)},
        ],
        "recent_runs": [run_service.to_dict(db, run) for run in published[:6]],
        "quota": auth_service.quota_snapshot(user, daily_limit),
        "job_status_counts": {
            str(row[0]): int(row[1])
            for row in db.execute(
                select(AlignmentRun.status, func.count(AlignmentRun.id)).group_by(AlignmentRun.status)
            ).all()
        },
        "pending_jobs": int(
            db.scalar(
                select(func.count(AlignmentJob.id)).where(AlignmentJob.status == JobStatus.PENDING.value)
            )
            or 0
        ),
    }

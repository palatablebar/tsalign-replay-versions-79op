from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.config.database import Base


class AlignmentRun(Base):
    """一次回填执行产出的独立结果版本。

    - ``version_no`` 在任务内递增，行一旦写定即不可变（重跑只新增版本）；
    - ``request_key`` 是幂等键：任务参数 + 输入数据指纹，重复请求只生效一次；
    - ``input_watermark`` 冻结执行时看到的输入证据，旧版本追溯不带回迟到数据；
    - ``published_at`` 非空表示该版本已成功发布（任务的 current 指针只指向这类版本）。
    """

    __tablename__ = "alignment_run"
    __table_args__ = (UniqueConstraint("job_id", "version_no", name="uq_alignment_run_job_version"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("alignment_job.id"), nullable=False, index=True)
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    request_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    coverage_pct: Mapped[float] = mapped_column(Float, default=0.0)
    drift_ms: Mapped[float] = mapped_column(Float, default=0.0)
    drift_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    anomaly_rate: Mapped[float] = mapped_column(Float, default=0.0)
    quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    gap_count: Mapped[int] = mapped_column(Integer, default=0)
    gap_total_s: Mapped[float] = mapped_column(Float, default=0.0)
    point_count: Mapped[int] = mapped_column(Integer, default=0)
    bucket_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_reason: Mapped[str] = mapped_column(String(40), default="")
    input_watermark: Mapped[list] = mapped_column(JSON, default=list)
    result_json: Mapped[dict] = mapped_column(JSON, default=dict)

    job = relationship("AlignmentJob", lazy="joined", foreign_keys=[job_id])

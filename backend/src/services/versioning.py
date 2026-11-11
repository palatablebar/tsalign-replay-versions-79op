"""Version identity for alignment runs.

每次回填执行产出一个独立的 ``AlignmentRun`` 版本。版本身份由两部分决定：

1. 任务参数（测点集 / 时间窗 / 网格 / 聚合与填补策略）；
2. 执行时实际看到的输入数据指纹（窗口内原始点的内容哈希）。

两者都相同 => 同一次回填请求 => ``request_key`` 相同 => 幂等命中，只生效一次；
源数据补传（迟到数据）会改变指纹 => 新的 ``request_key`` => 形成新版本。
指纹同时作为 ``input_watermark`` 落库，是该版本"冻结输入"的追溯证据：
旧版本永远按自己的快照读取，后来到达的源数据不会回流。
"""
from __future__ import annotations

import hashlib
import json
from typing import Iterable

Point = tuple[float, float, int]


def fingerprint_points(points: Iterable[Point]) -> dict:
    """Content fingerprint of one signal's in-window raw points.

    输入是 ``(ts, value, seq)`` 序列（通常已按窗口过滤）。排序规范化后做内容哈希，
    同一批点无论来自 ORM 查询还是 seed 内存序列，指纹都一致。
    """
    ordered = sorted((float(ts), float(value), int(seq)) for ts, value, seq in points)
    digest = hashlib.sha1()
    for ts, value, seq in ordered:
        digest.update(f"{ts!r}|{value!r}|{seq};".encode("utf-8"))
    return {
        "point_count": len(ordered),
        "first_ts": ordered[0][0] if ordered else None,
        "last_ts": ordered[-1][0] if ordered else None,
        "max_seq": max((seq for _ts, _value, seq in ordered), default=-1),
        "content_sha1": digest.hexdigest(),
    }


def build_watermark(named_points: Iterable[tuple[str, Iterable[Point]]]) -> list[dict]:
    """Per-signal frozen-input evidence stored on the run row.

    以业务唯一键 ``signal_code`` 标识测点（不用自增 id，保证 seed 与 worker
    两条路径对同一批数据算出相同指纹）。
    """
    watermark = []
    for signal_code, points in named_points:
        entry = {"signal_code": str(signal_code)}
        entry.update(fingerprint_points(points))
        watermark.append(entry)
    watermark.sort(key=lambda entry: entry["signal_code"])
    return watermark


def request_key(job, watermark: list[dict]) -> str:
    """Deterministic idempotency key for one backfill request.

    同一任务参数 + 同一份输入数据 => 同一个 key；重复执行只生效一次。
    """
    canonical = {
        "signal_codes": sorted(str(code) for code in (job.signal_codes or [])),
        "window_start": job.window_start.isoformat(),
        "window_end": job.window_end.isoformat(),
        "grid_s": int(job.grid_s),
        "agg_strategy": str(job.agg_strategy),
        "fill_strategy": str(job.fill_strategy),
        "inputs": watermark,
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

"""历史回补任务领域服务：历史区间回补、覆盖策略与失败区间重采。

管理历史时间序列数据的批量回补任务，支持日期区间分割、
并行采集、覆盖策略（追加/覆写）和失败区间追溯。

回补日期覆盖数据（backfill_coverage 分区）逐日记录覆盖明细：
已写入（有落湖回执）、确认为空（空页获源端确认）、空页未确认、
查询失败、待回补。未确认的空响应不计入完成水位，中断后续做时
仍能从覆盖数据中找到该日期续采，避免漏采；接续位置兼容历史
回补回执（完成水位/失败区间字段）。确认结果同步端点进度台账
（endpoint_progress 分区），两处日期范围取自同一份覆盖记录。

跨模块关联：incre_cursor（起始水位、可接续日期）、batch_ingest（批量拉取）、data_lake_sink（回补落湖）。
"""
from __future__ import annotations

import ast
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 回补日期覆盖数据分区：逐日覆盖明细的持久化存储
COVERAGE_MODULE = "backfill_coverage"
# 端点进度台账分区：回补确认结果的台账视图
LEDGER_MODULE = "endpoint_progress"

# 逐日覆盖状态
STATUS_WRITTEN = "已写入"            # 数据已落湖，有落湖回执
STATUS_CONFIRMED_EMPTY = "确认为空"   # 空页已获源端确认，无需重采
STATUS_UNCONFIRMED_EMPTY = "空页未确认"  # 空响应未获源端确认，需续采防漏采
STATUS_FAILED = "查询失败"            # 查询或落湖失败，需续采
STATUS_PENDING = "待回补"            # 尚未尝试

# 已解决：不需要再续采的状态
RESOLVED_STATUSES = {STATUS_WRITTEN, STATUS_CONFIRMED_EMPTY}
# 暂时未取到：可接续（待续采）的状态
UNRESOLVED_STATUSES = {STATUS_UNCONFIRMED_EMPTY, STATUS_FAILED, STATUS_PENDING}


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回补任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务编号", "关联端点", "起始日期", "截止日期", "并行度", "覆盖策略", "完成水位", "失败区间"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "起始日期"]
    STATUS_ORDER = ["草稿", "运行中", "已完成", "已取消"]
    ACTION_RULES = {"启动回补": "运行中", "确认完成": "已完成", "取消回补": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._failed_intervals: dict[int, list[dict[str, Any]]] = {}
        self._progress: dict[int, float] = {}
        # 模拟规则（确定性，便于测试与演示）：
        # 每月2日首次拉取返回未确认空页；每月4日返回源端确认空页
        self._sim_unconfirmed_empty_dom: set[int] = {2}
        self._sim_confirmed_empty_dom: set[int] = {4}

    # ──────────────────────────────────────────────
    # 回补执行
    # ──────────────────────────────────────────────
    def execute_backfill(self, job_id: int) -> dict[str, Any]:
        """执行历史数据回补任务（支持中断接续）。

        完整流程：
        1. 解析起始/截止日期区间
        2. 加载回补日期覆盖数据；缺失时从历史回补回执（完成水位/失败区间）还原
        3. 计算可接续日期（已写入/确认为空 之外的日期）
        4. 逐日拉取，按日记录覆盖明细（空页保留源端确认标记）
        5. 有数据则落湖并记录落湖回执
        6. 汇总同一份覆盖记录更新任务水位，并同步端点进度台账

        Returns:
            { ok, days_processed, records_backfilled, written_dates,
              unconfirmed_empty_dates, confirmed_empty_dates, failed_intervals, ... }
        """
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"回补任务 {job_id} 不存在", "days_processed": 0}

        if job.get("status") != "运行中":
            return {"ok": False, "message": f"任务状态{job['status']}，无法执行", "days_processed": 0}

        endpoint_id = self._parse_endpoint_id(job)
        overwrite_policy = str(job.get("覆盖策略", "追加不覆写"))
        days_list = self._job_days(job)
        if not days_list:
            return {"ok": False, "message": "日期格式无效", "days_processed": 0}

        # 获取增量水位作为起始参考
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            starting_position = cursor_svc.get_position(endpoint_id).get("position", 0)
        except Exception:
            starting_position = 0

        # 接续位置兼容历史回补回执：无覆盖数据时从任务字段还原
        self._seed_coverage_from_receipts(job, days_list, endpoint_id)
        coverage = self._load_coverage(job_id)

        # 可接续日期：未解决（空页未确认/查询失败/待回补），覆写策略下含已写入
        overwrite = "覆写" in overwrite_policy and "不覆写" not in overwrite_policy
        todo = []
        for date_str in days_list:
            rec = coverage.get(date_str)
            if rec is None or rec.get("status") in UNRESOLVED_STATUSES:
                todo.append(date_str)
            elif overwrite and rec.get("status") == STATUS_WRITTEN:
                todo.append(date_str)

        # 逐日回补并记录覆盖明细
        run = self._run_days(job_id, endpoint_id, todo)

        # 汇总：任务水位与台账的日期范围取自同一份覆盖记录
        coverage = self._load_coverage(job_id)
        summary = self._coverage_summary(coverage, days_list)
        resolved_count = summary["resolved"]
        is_complete = resolved_count == len(days_list)
        self._progress[job_id] = resolved_count / max(len(days_list), 1) * 100

        # 失败区间回执：仍未取到的日期（查询失败 + 空页未确认）
        open_intervals = [
            {"date": d, "reason": coverage[d]["status"]}
            for d in days_list
            if d in coverage and coverage[d].get("status") in (STATUS_FAILED, STATUS_UNCONFIRMED_EMPTY)
        ]
        self._failed_intervals[job_id] = open_intervals

        json_store.update(self.MODULE, job_id, {
            "完成水位": f"{resolved_count}/{len(days_list)}",
            "失败区间": str(open_intervals) if open_intervals else "无",
            "已覆盖范围": summary["range_label"],
            "status": "已完成" if is_complete else "运行中",
            "pending": not is_complete,
            "abnormal": len(open_intervals) > 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        # 回补确认结果同步到端点进度台账
        self._sync_endpoint_progress(endpoint_id)

        unconfirmed = run["unconfirmed_empty_dates"]
        message = f"回补完成: {resolved_count}/{len(days_list)} 天，{run['records_backfilled']} 条记录"
        if unconfirmed:
            message += f"，{len(unconfirmed)} 天空页未确认待续采"

        return {
            "ok": True,
            "message": message,
            "days_processed": resolved_count,
            "total_days": len(days_list),
            "days_attempted": len(todo),
            "records_backfilled": run["records_backfilled"],
            "written_dates": run["written_dates"],
            "confirmed_empty_dates": run["confirmed_empty_dates"],
            "unconfirmed_empty_dates": unconfirmed,
            "failed_intervals": open_intervals,
            "resumable_dates": summary["resumable_dates"],
            "coverage_range": summary["range_label"],
            "starting_position": starting_position,
            "progress_pct": round(resolved_count / max(len(days_list), 1) * 100, 2),
        }

    def _run_days(self, job_id: int, endpoint_id: int, dates: list[str]) -> dict[str, Any]:
        """逐日执行回补：拉取 → 按确认情况记录覆盖明细 → 落湖并留回执。"""
        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()

        coverage = self._load_coverage(job_id)
        written_dates: list[str] = []
        confirmed_empty_dates: list[str] = []
        unconfirmed_empty_dates: list[str] = []
        failed_intervals: list[dict[str, Any]] = []
        records_backfilled = 0

        for day_str in dates:
            attempt = int(coverage.get(day_str, {}).get("attempts", 0) or 0) + 1
            try:
                fetch = self._mock_fetch_day(endpoint_id, day_str, random.randint(10, 50), attempt=attempt)
            except Exception as e:
                self._upsert_coverage(job_id, endpoint_id, day_str,
                                      STATUS_FAILED, False, 0, f"查询异常: {e}")
                failed_intervals.append({"date": day_str, "reason": str(e)})
                continue

            if not fetch.get("ok", True):
                reason = str(fetch.get("message", "查询失败"))
                self._upsert_coverage(job_id, endpoint_id, day_str,
                                      STATUS_FAILED, False, 0, reason)
                failed_intervals.append({"date": day_str, "reason": reason})
                continue

            records = fetch.get("records") or []
            if records:
                # 落湖（跨模块：data_lake_sink），保留落湖回执
                try:
                    sink_result = sink_svc.write_batch(records, event_type="backfill",
                                                       source_channel_id=endpoint_id)
                except Exception as e:
                    sink_result = {"ok": False, "message": str(e)}
                written = int(sink_result.get("written", 0) or 0)
                if sink_result.get("ok") and written > 0:
                    receipt = sink_result.get("partition") or sink_result.get("file_path") or "已落湖"
                    self._upsert_coverage(job_id, endpoint_id, day_str,
                                          STATUS_WRITTEN, True, written, str(receipt))
                    records_backfilled += written
                    written_dates.append(day_str)
                else:
                    reason = str(sink_result.get("message", "落湖失败"))
                    self._upsert_coverage(job_id, endpoint_id, day_str,
                                          STATUS_FAILED, False, 0, reason)
                    failed_intervals.append({"date": day_str, "reason": reason})
            elif fetch.get("confirmed"):
                # 源端确认空页：当日无数据已确认，无需重采
                self._upsert_coverage(job_id, endpoint_id, day_str,
                                      STATUS_CONFIRMED_EMPTY, True, 0,
                                      str(fetch.get("message", "源端确认当日无数据")))
                confirmed_empty_dates.append(day_str)
            else:
                # 未确认空页：保留日期与确认标记，中断后续做仍能找到该日期
                self._upsert_coverage(job_id, endpoint_id, day_str,
                                      STATUS_UNCONFIRMED_EMPTY, False, 0,
                                      str(fetch.get("message", "空响应未获源端确认")))
                unconfirmed_empty_dates.append(day_str)

        return {
            "written_dates": written_dates,
            "confirmed_empty_dates": confirmed_empty_dates,
            "unconfirmed_empty_dates": unconfirmed_empty_dates,
            "failed_intervals": failed_intervals,
            "records_backfilled": records_backfilled,
        }

    # ──────────────────────────────────────────────
    # 回补日期覆盖数据
    # ──────────────────────────────────────────────
    def _load_coverage(self, job_id: int) -> dict[str, dict[str, Any]]:
        """加载任务的回补日期覆盖数据（date -> 覆盖记录）。"""
        return {
            str(r.get("date")): r
            for r in json_store.get_all(COVERAGE_MODULE)
            if int(r.get("job_id", 0) or 0) == job_id
        }

    def _load_endpoint_coverage(self, endpoint_id: int) -> list[dict[str, Any]]:
        """加载端点维度的全部覆盖记录（供端点进度台账与增量进度使用）。"""
        return [
            r for r in json_store.get_all(COVERAGE_MODULE)
            if int(r.get("endpoint_id", 0) or 0) == endpoint_id
        ]

    def _upsert_coverage(self, job_id: int, endpoint_id: int, date_str: str,
                         status: str, source_confirmed: bool, records: int,
                         receipt: str) -> dict[str, Any]:
        """按 (任务, 日期) 更新或插入覆盖记录，保留空页的源端确认标记。"""
        existing = self._load_coverage(job_id).get(date_str)
        now = datetime.now(timezone.utc).isoformat()
        if existing:
            updates = {
                "status": status,
                "source_confirmed": source_confirmed,
                "records": records,
                "receipt": receipt,
                "attempts": int(existing.get("attempts", 0) or 0) + 1,
                "updated_at": now,
            }
            json_store.update(COVERAGE_MODULE, int(existing["id"]), updates)
            return {**existing, **updates}
        entry = {
            "job_id": job_id,
            "endpoint_id": endpoint_id,
            "date": date_str,
            "status": status,
            "source_confirmed": source_confirmed,
            "records": records,
            "receipt": receipt,
            "attempts": 1,
            "updated_at": now,
        }
        return json_store.insert(COVERAGE_MODULE, entry)

    def _coverage_summary(self, coverage: dict[str, dict[str, Any]],
                          days_list: list[str]) -> dict[str, Any]:
        """汇总覆盖记录：完成水位、可接续日期与覆盖范围。

        任务完成水位与端点进度台账的日期范围都取自这一份覆盖记录。
        """
        resolved_dates = sorted(
            d for d in days_list if coverage.get(d, {}).get("status") in RESOLVED_STATUSES
        )
        resumable_dates = [
            d for d in days_list if coverage.get(d, {}).get("status") not in RESOLVED_STATUSES
        ]
        by_status: dict[str, int] = {}
        for d in days_list:
            status = coverage.get(d, {}).get("status", STATUS_PENDING)
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "resolved": len(resolved_dates),
            "total": len(days_list),
            "resolved_dates": resolved_dates,
            "resumable_dates": resumable_dates,
            "range_label": f"{resolved_dates[0]} ~ {resolved_dates[-1]}" if resolved_dates else "无",
            "by_status": by_status,
        }

    def _seed_coverage_from_receipts(self, job: dict[str, Any], days_list: list[str],
                                     endpoint_id: int) -> None:
        """兼容历史回补回执：从 完成水位/失败区间 还原逐日覆盖记录。

        历史回执只记录在任务字段上（完成水位 "N/M"、失败区间
        "[{'date':...,'reason':...}]"），没有逐日覆盖数据；续做时据此
        还原日期依据，保证失败恢复能找到具体日期，避免漏采。
        """
        job_id = int(job["id"])
        if self._load_coverage(job_id):
            return  # 已有覆盖数据，直接以覆盖数据为准

        # 1) 完成水位 "N/M"：前 N 天按历史回执视为已写入
        done = self._parse_watermark(str(job.get("完成水位", "") or ""))
        if done:
            for date_str in days_list[:done]:
                self._upsert_coverage(job_id, endpoint_id, date_str,
                                      STATUS_WRITTEN, True, 0, "历史回执:完成水位")

        # 2) 失败区间：失败日期改记查询失败（优先于水位回执），续做时仍能找到该日期
        failed_dates = self._parse_failed_intervals(str(job.get("失败区间", "") or ""))
        if failed_dates:
            seeded = self._load_coverage(job_id)
            for date_str in failed_dates:
                rec = seeded.get(date_str)
                if rec is None:
                    self._upsert_coverage(job_id, endpoint_id, date_str,
                                          STATUS_FAILED, False, 0, "历史回执:失败区间")
                elif str(rec.get("receipt", "")).startswith("历史回执"):
                    json_store.update(COVERAGE_MODULE, int(rec["id"]), {
                        "status": STATUS_FAILED,
                        "source_confirmed": False,
                        "receipt": "历史回执:失败区间",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })

    def _parse_watermark(self, raw: str) -> int | None:
        """解析历史回执中的完成水位 "N/M"，返回已完成天数 N。"""
        raw = (raw or "").strip()
        if "/" not in raw:
            return None
        try:
            return int(raw.split("/", 1)[0].strip())
        except ValueError:
            return None

    def _parse_failed_intervals(self, raw: str) -> list[str]:
        """解析历史回执中的失败区间字段，返回日期列表。"""
        raw = (raw or "").strip()
        if not raw or raw == "无":
            return []
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            return []
        dates: list[str] = []
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and item.get("date"):
                    dates.append(str(item["date"]))
                elif isinstance(item, str) and item.strip():
                    dates.append(item.strip())
        return dates

    # ──────────────────────────────────────────────
    # 端点进度台账
    # ──────────────────────────────────────────────
    def _sync_endpoint_progress(self, endpoint_id: int) -> dict[str, Any]:
        """把回补确认结果同步到端点进度台账。

        台账的覆盖日期范围与任务完成水位取自同一份回补日期覆盖记录。
        """
        records = self._load_endpoint_coverage(endpoint_id)
        resolved = sorted(str(r.get("date")) for r in records
                          if r.get("status") in RESOLVED_STATUSES)
        by_status: dict[str, int] = {}
        for r in records:
            status = str(r.get("status", STATUS_PENDING))
            by_status[status] = by_status.get(status, 0) + 1

        payload = {
            "endpoint_id": endpoint_id,
            "覆盖起始日期": resolved[0] if resolved else "",
            "覆盖截止日期": resolved[-1] if resolved else "",
            "已写入天数": by_status.get(STATUS_WRITTEN, 0),
            "确认为空天数": by_status.get(STATUS_CONFIRMED_EMPTY, 0),
            "未解决天数": sum(by_status.get(s, 0) for s in UNRESOLVED_STATUSES),
            "覆盖日期总数": len(records),
            "来源回补任务": sorted({int(r.get("job_id", 0) or 0) for r in records}),
            "最近同步时间": datetime.now(timezone.utc).isoformat(),
        }

        existing = next(
            (r for r in json_store.get_all(LEDGER_MODULE)
             if int(r.get("endpoint_id", 0) or 0) == endpoint_id),
            None,
        )
        if existing:
            json_store.update(LEDGER_MODULE, int(existing["id"]), payload)
            payload["id"] = existing["id"]
        else:
            inserted = json_store.insert(LEDGER_MODULE, payload)
            payload["id"] = inserted["id"]
        return payload

    def get_endpoint_progress(self, endpoint_id: int) -> dict[str, Any]:
        """查询端点进度台账（回补确认结果的台账视图）。"""
        rows = [
            r for r in json_store.get_all(LEDGER_MODULE)
            if int(r.get("endpoint_id", 0) or 0) == endpoint_id
        ]
        if not rows:
            return {"ok": False, "message": f"端点 {endpoint_id} 暂无进度台账",
                    "endpoint_id": endpoint_id}
        return {"ok": True, "endpoint_id": endpoint_id, "ledger": rows[0]}

    # ──────────────────────────────────────────────
    # 覆盖明细查询与缺口确认
    # ──────────────────────────────────────────────
    def get_date_coverage(self, job_id: int) -> dict[str, Any]:
        """回填覆盖明细：逐日覆盖状态与区间划分。

        从回补日期覆盖数据可区分：
        - 已写入：拿到落湖回执；
        - 确认为空：空页已获源端确认；
        - 暂时未取到：空页未确认 / 查询失败 / 待回补（中断后续做仍能找到该日期）。
        """
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}
        days_list = self._job_days(job)
        coverage = self._load_coverage(job_id)

        details = []
        for date_str in days_list:
            rec = coverage.get(date_str)
            if rec:
                details.append({
                    "date": date_str,
                    "status": rec.get("status"),
                    "source_confirmed": bool(rec.get("source_confirmed")),
                    "records": int(rec.get("records", 0) or 0),
                    "receipt": rec.get("receipt", ""),
                    "attempts": int(rec.get("attempts", 0) or 0),
                })
            else:
                details.append({
                    "date": date_str,
                    "status": STATUS_PENDING,
                    "source_confirmed": False,
                    "records": 0,
                    "receipt": "",
                    "attempts": 0,
                })

        intervals = {
            STATUS_WRITTEN: [d["date"] for d in details if d["status"] == STATUS_WRITTEN],
            STATUS_CONFIRMED_EMPTY: [d["date"] for d in details if d["status"] == STATUS_CONFIRMED_EMPTY],
            "暂时未取到": [
                {"date": d["date"], "status": d["status"]}
                for d in details if d["status"] in UNRESOLVED_STATUSES
            ],
        }
        summary = self._coverage_summary(coverage, days_list)
        return {
            "ok": True,
            "job_id": job_id,
            "endpoint_id": self._parse_endpoint_id(job),
            "details": details,
            "intervals": intervals,
            "summary": summary,
        }

    def verify_gaps(self, job_id: int) -> dict[str, Any]:
        """按各天的落湖回执确认缺口是否补齐。

        已写入：有落湖回执；确认为空：有源端确认；其余日期为未补齐缺口。
        """
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}
        days_list = self._job_days(job)
        coverage = self._load_coverage(job_id)

        filled: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []
        for date_str in days_list:
            rec = coverage.get(date_str)
            status = rec.get("status") if rec else STATUS_PENDING
            if status == STATUS_WRITTEN and rec and rec.get("receipt"):
                filled.append({
                    "date": date_str,
                    "via": "落湖回执",
                    "receipt": rec.get("receipt"),
                    "records": int(rec.get("records", 0) or 0),
                })
            elif status == STATUS_CONFIRMED_EMPTY:
                filled.append({
                    "date": date_str,
                    "via": "源端确认空页",
                    "receipt": rec.get("receipt", "") if rec else "",
                    "records": 0,
                })
            else:
                gaps.append({"date": date_str, "status": status})

        return {
            "ok": True,
            "job_id": job_id,
            "total_days": len(days_list),
            "filled": filled,
            "gaps": gaps,
            "all_filled": len(gaps) == 0,
        }

    # ──────────────────────────────────────────────
    # 失败区间重试
    # ──────────────────────────────────────────────
    def retry_failed_intervals(self, job_id: int) -> dict[str, Any]:
        """重试失败区间：以回补日期覆盖数据为准，兼容历史回补回执。

        空页未确认与查询失败的日期都会被续采；重试后按各天落湖回执
        更新覆盖状态，并同步端点进度台账。
        """
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        endpoint_id = self._parse_endpoint_id(job)
        days_list = self._job_days(job)

        # 兼容历史回补回执：无覆盖数据时从任务字段还原
        self._seed_coverage_from_receipts(job, days_list, endpoint_id)
        coverage = self._load_coverage(job_id)

        # 可接续日期 = 覆盖数据中未解决的日期（空页未确认/查询失败）
        targets = [
            d for d in days_list
            if coverage.get(d, {}).get("status") in (STATUS_UNCONFIRMED_EMPTY, STATUS_FAILED)
        ]
        # 兼容内存中的历史失败记录（旧实例未持久化时）
        for item in self._failed_intervals.get(job_id, []):
            date_str = str(item.get("date", ""))
            if (date_str in days_list and date_str not in targets
                    and coverage.get(date_str, {}).get("status") not in RESOLVED_STATUSES):
                targets.append(date_str)
        targets.sort()

        if not targets:
            return {"ok": True, "message": "无失败区间需要重试", "retried": 0, "still_failed": 0}

        run = self._run_days(job_id, endpoint_id, targets)

        # 重试后按落湖回执刷新任务水位与台账
        coverage = self._load_coverage(job_id)
        summary = self._coverage_summary(coverage, days_list)
        still_open = [
            d for d in days_list
            if coverage.get(d, {}).get("status") in (STATUS_FAILED, STATUS_UNCONFIRMED_EMPTY)
        ]
        self._failed_intervals[job_id] = [
            {"date": d, "reason": coverage[d]["status"]} for d in still_open
        ]
        is_complete = summary["resolved"] == len(days_list)
        self._progress[job_id] = summary["resolved"] / max(len(days_list), 1) * 100
        json_store.update(self.MODULE, job_id, {
            "完成水位": f"{summary['resolved']}/{len(days_list)}",
            "失败区间": str(self._failed_intervals[job_id]) if still_open else "无",
            "已覆盖范围": summary["range_label"],
            "status": "已完成" if is_complete else str(job.get("status", "运行中")),
            "pending": not is_complete,
            "abnormal": len(still_open) > 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        self._sync_endpoint_progress(endpoint_id)

        resolved_this_round = len([d for d in targets if d not in still_open])
        return {
            "ok": True,
            "message": f"重试完成: 成功 {resolved_this_round}/{len(targets)}",
            "retried": len(targets),
            "resolved": resolved_this_round,
            "still_failed": sum(1 for d in still_open
                                if coverage[d].get("status") == STATUS_FAILED),
            "still_unconfirmed": sum(1 for d in still_open
                                     if coverage[d].get("status") == STATUS_UNCONFIRMED_EMPTY),
            "written_dates": run["written_dates"],
            "confirmed_empty_dates": run["confirmed_empty_dates"],
            "unconfirmed_empty_dates": run["unconfirmed_empty_dates"],
        }

    # ──────────────────────────────────────────────
    # 进度查询
    # ──────────────────────────────────────────────
    def get_backfill_progress(self, job_id: int) -> dict[str, Any]:
        """获取回补任务进度（含覆盖汇总与可接续日期）。"""
        job = json_store.get_by_id(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": "任务不存在"}

        days_list = self._job_days(job)
        coverage = self._load_coverage(job_id)
        summary = self._coverage_summary(coverage, days_list)
        progress_pct = self._progress.get(job_id)
        if progress_pct is None:
            progress_pct = summary["resolved"] / max(len(days_list), 1) * 100 if days_list else 0

        return {
            "ok": True,
            "job_id": job_id,
            "job_no": job.get("任务编号"),
            "status": job.get("status"),
            "progress_pct": progress_pct,
            "failed_intervals": len(self._failed_intervals.get(job_id, [])),
            "endpoint": job.get("关联端点"),
            "date_range": f"{job.get('起始日期')} ~ {job.get('截止日期')}",
            "resolved_days": summary["resolved"],
            "resumable_dates": summary["resumable_dates"],
            "coverage_range": summary["range_label"],
        }

    # ──────────────────────────────────────────────
    # 工具方法
    # ──────────────────────────────────────────────
    def _job_days(self, job: dict[str, Any]) -> list[str]:
        """解析任务的起止日期，展开为逐日列表。"""
        start_date = self._parse_date(str(job.get("起始日期", "")))
        end_date = self._parse_date(str(job.get("截止日期", "")))
        if start_date is None or end_date is None:
            return []
        days_list = []
        current = start_date
        while current <= end_date:
            days_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return days_list

    def _parse_endpoint_id(self, job: dict[str, Any]) -> int:
        """解析关联端点ID，非法值回退为0。"""
        try:
            return int(job.get("关联端点", "0") or "0")
        except (TypeError, ValueError):
            return 0

    def _parse_date(self, date_str: str) -> datetime | None:
        """解析日期字符串（支持多种格式）。"""
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _mock_fetch_day(self, endpoint_id: int, day: str, count: int,
                        attempt: int = 1) -> dict[str, Any]:
        """模拟按日拉取来源端点的日分区数据。

        返回 {ok, confirmed, records, message}：
        - ok=False：查询失败（源端异常）；
        - records 非空：正常数据页；
        - records 为空且 confirmed=True：源端确认当日无数据（确认为空）；
        - records 为空且 confirmed=False：空响应未获源端确认（未确认空页，需续采）。

        模拟规则（确定性，便于测试）：每月2日首次拉取返回未确认空页，
        每月4日返回确认为空，重试（attempt>=2）及其余日期正常返回数据。
        """
        try:
            day_of_month = int(day[-2:])
        except (ValueError, IndexError):
            day_of_month = 0

        if attempt < 2 and day_of_month in self._sim_unconfirmed_empty_dom:
            return {"ok": True, "confirmed": False, "records": [],
                    "message": "空响应未获源端确认"}
        if day_of_month in self._sim_confirmed_empty_dom:
            return {"ok": True, "confirmed": True, "records": [],
                    "message": "源端确认当日无数据"}

        records = [
            {
                "id": i + 1,
                "date": day,
                "endpoint_id": endpoint_id,
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "timestamp": f"{day}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            }
            for i in range(count)
        ]
        return {"ok": True, "confirmed": True, "records": records, "message": "ok"}

    def domain_snapshot(self) -> dict[str, Any]:
        """输出历史回补任务的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        coverage_rows = json_store.get_all(COVERAGE_MODULE)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "历史回补任务数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_jobs": sum(1 for r in rows if r.get("status") == "运行中"),
            "completed_jobs": sum(1 for r in rows if r.get("status") == "已完成"),
            "coverage_days": len(coverage_rows),
            "unconfirmed_empty_days": sum(1 for r in coverage_rows
                                          if r.get("status") == STATUS_UNCONFIRMED_EMPTY),
            "confirmed_empty_days": sum(1 for r in coverage_rows
                                        if r.get("status") == STATUS_CONFIRMED_EMPTY),
            "operation": self.OPERATION_TERM,
        }

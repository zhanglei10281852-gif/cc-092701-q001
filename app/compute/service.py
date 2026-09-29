from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import (
    ConflictError,
    LeaseExpiredError,
    LeaseLostError,
    NotFoundError,
    ReceiptConflictError,
    TaskClosedError,
    ValidationError,
    VersionConflictError,
)
from app.database import get_connection, transaction

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def new_lease_token() -> str:
    return secrets.token_hex(16)


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["lease_events"] = self.repository.lease_events(task_id)
        result["consistency"] = self._consistency_report(result)
        return result

    @staticmethod
    def _consistency_report(task: dict[str, Any]) -> dict[str, Any]:
        """交叉核对状态、工作者身份与成绩版本，读回结果必须自洽。"""
        status = task["status"]
        results = task["results"]
        max_version = max((int(item["version"]) for item in results), default=None)
        current_version = task["current_result_version"]
        checks: dict[str, bool] = {
            "status_valid": status in {"queued", "running", "cancel_requested", "cancelled", "succeeded", "failed"},
            "result_versions_contiguous": sorted(int(item["version"]) for item in results) == list(range(1, len(results) + 1)),
        }
        if status == "succeeded":
            checks["final_version_present"] = current_version is not None and current_version == max_version
            final = next((item for item in results if int(item["version"]) == current_version), None)
            checks["final_identity_matches"] = (
                final is not None and bool(task["lease_owner"]) is False and bool(task["lease_token"]) is False
                and (final["created_by"] or "") != ""
            )
        elif status == "running":
            checks["lease_identity_held"] = bool(task["lease_owner"]) and bool(task["lease_token"])
        else:
            checks["lease_released"] = not task["lease_owner"] and not task["lease_token"]
        checks["consistent"] = all(checks.values())
        return checks

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            lease_token = new_lease_token()
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_token=?,"
                "lease_expires_at=?,fencing_epoch=fencing_epoch+1,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 "
                "WHERE id=? AND status='queued'",
                (worker_id, lease_token, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            task = dict(repository.task_by_id(candidate["id"]))
            repository.add_lease_event(
                task_id=candidate["id"], event_type="lease_granted", worker_id=worker_id,
                lease_token=lease_token, fencing_epoch=task["fencing_epoch"], task_version=task["version"], now=now,
            )
            return self._attach_lease(task)

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int,
                  lease_token: str, observed_version: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = self._require_task(repository, task_id)
            violation = self._lease_violation(task, worker_id, lease_token, observed_version, now_value)
            if violation is not None:
                self._record_rejection(repository, task, violation, lease_token, worker_id, now)
                error = violation["error"]
            else:
                cursor = connection.execute(
                    "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (expires, now, task_id),
                )
                if cursor.rowcount != 1:  # pragma: no cover - 由上面的守卫保证
                    raise ConflictError("任务未由当前工作者持有")
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="heartbeat", worker_id=worker_id,
                    lease_token=lease_token, fencing_epoch=after["fencing_epoch"], task_version=after["version"], now=now,
                )
                value = self._attach_lease(after)
                error = None
        if error is not None:
            raise error
        return value

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any],
                 lease_token: str, observed_version: int, receipt_key: str = "") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = self._require_task(repository, task_id)
            # 幂等：同一回执键的合法重试必须返回首次结果，不能重复写入或重复审计。
            if receipt_key:
                existing = repository.result_by_receipt(task_id, receipt_key)
                if existing is not None:
                    if task["status"] == "succeeded" and task["current_result_version"] == existing["version"]:
                        return self._replay_result(repository, dict(task), existing, result, metrics, worker_id)
                    # 回执键属于已被人工重试取代的旧尝试，不能冒充新一轮结果。
                    raise ReceiptConflictError("回执键属于已经被重试取代的旧尝试，请使用新的回执键")
            violation = self._lease_violation(task, worker_id, lease_token, observed_version, now_value)
            if violation is not None:
                self._record_rejection(repository, task, violation, lease_token, worker_id, now)
                error = violation["error"]
            else:
                version = int(connection.execute(
                    "SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)
                ).fetchone()[0])
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,receipt_key,lease_token,fencing_epoch,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True),
                     json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                     digest({"result": result, "metrics": metrics}), receipt_key, lease_token,
                     task["fencing_epoch"], worker_id, now),
                )
                connection.execute(
                    "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_token='',"
                    "lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (version, now, now, task_id),
                )
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="result_accepted", worker_id=worker_id,
                    lease_token=lease_token, fencing_epoch=task["fencing_epoch"], task_version=after["version"],
                    result_version=version, now=now,
                )
                value = self._attach_lease(after)
                error = None
        if error is not None:
            raise error
        return value

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool,
             lease_token: str, observed_version: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = self._require_task(repository, task_id)
            violation = self._lease_violation(task, worker_id, lease_token, observed_version, now_value)
            if violation is not None:
                self._record_rejection(repository, task, violation, lease_token, worker_id, now)
                error = violation["error"]
            else:
                can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
                status = "queued" if can_retry else "failed"
                delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
                available = to_storage(now_value + timedelta(seconds=delay))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_token='',lease_expires_at='',"
                    "last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
                )
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="failure_accepted", worker_id=worker_id,
                    lease_token=lease_token, fencing_epoch=task["fencing_epoch"], task_version=after["version"],
                    detail=f"{error_code}:{'retry' if can_retry else 'terminal'}", now=now,
                )
                value = self._attach_lease(after)
                error = None
        if error is not None:
            raise error
        return value

    # ---- 租约守卫 -------------------------------------------------------

    @staticmethod
    def _require_task(repository: ComputeRepository, task_id: int) -> sqlite3.Row:
        task = repository.task_by_id(task_id)
        if task is None:
            raise NotFoundError("计算任务不存在")
        return task

    def _lease_violation(self, task: sqlite3.Row, worker_id: str, lease_token: str,
                         observed_version: int, now_value: datetime) -> dict[str, Any] | None:
        """每次心跳/回执都必须同时证明：任务仍在运行、持有当前租约令牌、租约未到期、版本未被推进。"""
        status = task["status"]
        if status in TERMINAL_STATUSES:
            return {"reason": f"task_{status}", "error": TaskClosedError("任务已经结束，迟到的工作者回执不再被接受")}
        if status != "running":
            return {"reason": f"task_{status}", "error": LeaseLostError("任务当前不在运行状态，租约已经失效")}
        if not task["lease_token"] or task["lease_token"] != lease_token or task["lease_owner"] != worker_id:
            # 令牌不匹配：租约已被恢复流程撤销，或被另一个会话接管。
            return {"reason": "token_mismatch", "error": LeaseLostError("租约凭证无效：任务可能已经被其他工作者接管")}
        expires = from_storage(task["lease_expires_at"])
        if expires is not None and now_value > expires:
            # 凭证正确但租约已过期：旧会话不能靠心跳自救，必须停止提交并等待恢复接管。
            return {"reason": "lease_expired", "error": LeaseExpiredError("租约已经过期，工作者必须停止提交并等待重新领取")}
        if int(observed_version) != int(task["version"]):
            return {
                "reason": "stale_version",
                "error": VersionConflictError(
                    "任务版本已经变化，请重新读取状态后再提交",
                    context={"observed_version": int(observed_version), "current_version": int(task["version"])},
                ),
            }
        return None

    @staticmethod
    def _record_rejection(repository: ComputeRepository, task: sqlite3.Row, violation: dict[str, Any],
                          claimed_token: str, claimed_worker: str, now: str) -> None:
        """拒绝事件与守卫判定在同一事务内落库并随事务提交，时间线包含每一次拒绝。"""
        repository.add_lease_event(
            task_id=int(task["id"]), event_type="lease_rejected",
            worker_id=claimed_worker or str(task["lease_owner"]),
            lease_token=claimed_token or str(task["lease_token"]),
            fencing_epoch=int(task["fencing_epoch"]), task_version=int(task["version"]),
            detail=violation["reason"], now=now,
        )

    def _replay_result(self, repository: ComputeRepository, task: dict[str, Any], existing: sqlite3.Row,
                       result: dict[str, Any], metrics: dict[str, Any], worker_id: str) -> dict[str, Any]:
        if existing["created_by"] != worker_id or existing["result_digest"] != digest({"result": result, "metrics": metrics}):
            raise ReceiptConflictError("同一回执键已经对应另一位工作者或另一份结果内容")
        # 幂等重放不产生新的结果版本，也不新增审计事件。
        return self._attach_lease(task)

    @staticmethod
    def _attach_lease(task: dict[str, Any]) -> dict[str, Any]:
        task["lease_token"] = task.get("lease_token", "")
        task["fencing_epoch"] = int(task.get("fencing_epoch") or 0)
        return task

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_token='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute(
                "SELECT * FROM compute_tasks WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
                (now,),
            ).fetchall()
            for task in rows:
                before = dict(task)
                if task["status"] == "cancel_requested":
                    status, finished_at, error_code, error_message = "cancelled", now, "lease_expired", "取消请求期间租约过期，任务已取消"
                    cancelled.append(int(task["id"]))
                elif int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at, error_code, error_message = "queued", None, "lease_expired", "工作者租约已过期"
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at, error_code, error_message = "failed", now, "lease_expired", "工作者租约已过期"
                    exhausted.append(int(task["id"]))
                # 撤销租约：清空令牌并推进 fencing 纪元，旧会话的任何凭证从此必然失效。
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_token='',lease_expires_at='',fencing_epoch=fencing_epoch+1,"
                    "available_at=?,last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, error_code, error_message, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_lease_event(
                    task_id=int(task["id"]), event_type="lease_reclaimed", worker_id=str(task["lease_owner"]),
                    lease_token=str(task["lease_token"]), fencing_epoch=int(task["fencing_epoch"]),
                    task_version=after["version"], detail=status, now=now,
                )
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized

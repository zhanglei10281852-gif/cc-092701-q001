from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, LeaseConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class _LeaseRejected(Exception):
    """租约校验未通过的内部信号，触发事务回滚后再落拒绝事件。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


_REJECTION_MESSAGES = {
    "not_running": "任务已不在运行中，租约随终态结束",
    "lease_expired": "租约已经过期，失联期间任务可能已被接管",
    "owner_changed": "任务已由其他工作者接管",
    "epoch_stale": "出示的租约代次已过期，这是接管前的旧会话",
    "version_stale": "出示的任务版本已过期，请按最新状态重试",
    "receipt_conflict": "同一回执键被用于内容不同的请求",
}


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
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_epoch=lease_epoch+1,"
                "lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            task = dict(repository.task_by_id(candidate["id"]))
            repository.add_lease_event(
                task_id=task["id"], event_type="granted", lease_epoch=int(task["lease_epoch"]), actor=worker_id,
                detail={"lease_seconds": lease_seconds, "lease_expires_at": lease_until, "attempt_count": task["attempt_count"]}, now=now,
            )
            return task

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int, lease_epoch: int, expected_version: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                self._authorize_lease(task, worker_id, lease_epoch, expected_version, now)
                cursor = connection.execute(
                    "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 "
                    "WHERE id=? AND status='running' AND lease_owner=? AND lease_epoch=? AND version=?",
                    (expires, now, task_id, worker_id, lease_epoch, task["version"]),
                )
                if cursor.rowcount != 1:
                    raise _LeaseRejected("owner_changed")
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="renewed", lease_epoch=lease_epoch, actor=worker_id,
                    detail={"lease_expires_at": expires, "version": after["version"]}, now=now,
                )
                return after
        except _LeaseRejected as rejected:
            self._record_rejection(task_id, worker_id, lease_epoch, expected_version, "heartbeat", rejected.reason, now)
            raise self._lease_error(rejected.reason, lease_epoch, expected_version) from rejected

    def complete(
        self,
        task_id: int,
        worker_id: str,
        result: dict[str, Any],
        metrics: dict[str, Any],
        *,
        lease_epoch: int,
        expected_version: int,
        receipt_id: str,
    ) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        request_digest = digest({"kind": "complete", "result": result, "metrics": metrics})
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                replay = repository.receipt(task_id, lease_epoch, receipt_id)
                if replay is not None:
                    if repository.epoch_owner(task_id, lease_epoch) != worker_id:
                        raise _LeaseRejected("owner_changed")
                    if replay["kind"] != "complete" or replay["request_digest"] != request_digest:
                        raise _LeaseRejected("receipt_conflict")
                    return json.loads(replay["response_json"])
                self._authorize_lease(task, worker_id, lease_epoch, expected_version, now)
                result_version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (task_id, result_version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
                )
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',"
                    "finished_at=?,updated_at=?,version=version+1 WHERE id=? AND lease_epoch=? AND version=?",
                    (result_version, now, now, task_id, lease_epoch, task["version"]),
                )
                if cursor.rowcount != 1:
                    raise _LeaseRejected("owner_changed")
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="succeeded", lease_epoch=lease_epoch, actor=worker_id,
                    detail={"receipt_id": receipt_id, "result_version": result_version, "version": after["version"]}, now=now,
                )
                repository.save_receipt(
                    task_id=task_id, lease_epoch=lease_epoch, receipt_id=receipt_id, kind="complete",
                    request_digest=request_digest, response=after, now=now,
                )
                return after
        except _LeaseRejected as rejected:
            self._record_rejection(task_id, worker_id, lease_epoch, expected_version, "complete", rejected.reason, now)
            raise self._lease_error(rejected.reason, lease_epoch, expected_version) from rejected

    def fail(
        self,
        task_id: int,
        worker_id: str,
        error_code: str,
        message: str,
        retryable: bool,
        *,
        lease_epoch: int,
        expected_version: int,
        receipt_id: str,
    ) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        request_digest = digest({"kind": "fail", "error_code": error_code, "message": message, "retryable": retryable})
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                if task is None:
                    raise NotFoundError("计算任务不存在")
                replay = repository.receipt(task_id, lease_epoch, receipt_id)
                if replay is not None:
                    if repository.epoch_owner(task_id, lease_epoch) != worker_id:
                        raise _LeaseRejected("owner_changed")
                    if replay["kind"] != "fail" or replay["request_digest"] != request_digest:
                        raise _LeaseRejected("receipt_conflict")
                    return json.loads(replay["response_json"])
                self._authorize_lease(task, worker_id, lease_epoch, expected_version, now)
                can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
                new_status = "queued" if can_retry else "failed"
                delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
                available = to_storage(now_value + timedelta(seconds=delay))
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,"
                    "last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=? AND lease_epoch=? AND version=?",
                    (new_status, available, error_code, message[:2000], None if can_retry else now, now, task_id, lease_epoch, task["version"]),
                )
                if cursor.rowcount != 1:
                    raise _LeaseRejected("owner_changed")
                after = dict(repository.task_by_id(task_id))
                repository.add_lease_event(
                    task_id=task_id, event_type="requeued" if can_retry else "failed", lease_epoch=lease_epoch, actor=worker_id,
                    detail={"receipt_id": receipt_id, "error_code": error_code, "retryable": retryable,
                            "backoff_seconds": delay, "version": after["version"]}, now=now,
                )
                repository.save_receipt(
                    task_id=task_id, lease_epoch=lease_epoch, receipt_id=receipt_id, kind="fail",
                    request_digest=request_digest, response=after, now=now,
                )
                return after
        except _LeaseRejected as rejected:
            self._record_rejection(task_id, worker_id, lease_epoch, expected_version, "fail", rejected.reason, now)
            raise self._lease_error(rejected.reason, lease_epoch, expected_version) from rejected

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
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
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<=? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                stale_epoch = int(task["lease_epoch"])
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',"
                    "last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1,lease_epoch=lease_epoch+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
                repository.add_lease_event(
                    task_id=task["id"], event_type="recovered", lease_epoch=stale_epoch, actor=actor,
                    detail={"stale_owner": before["lease_owner"], "stale_epoch": stale_epoch,
                            "new_status": status, "new_epoch": after["lease_epoch"]}, now=now,
                )
        return {"recovered": recovered, "exhausted": exhausted}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    @staticmethod
    def _authorize_lease(task: sqlite3.Row, worker_id: str, lease_epoch: int, expected_version: int, now: str) -> None:
        """校验调用方仍持有当前租约与版本，任一不符即抛出可区分的拒绝原因。"""
        if task["status"] != "running":
            raise _LeaseRejected("not_running")
        if task["lease_owner"] != worker_id:
            raise _LeaseRejected("owner_changed")
        if int(task["lease_epoch"]) != int(lease_epoch):
            raise _LeaseRejected("epoch_stale")
        if str(task["lease_expires_at"]) <= now:
            raise _LeaseRejected("lease_expired")
        if int(task["version"]) != int(expected_version):
            raise _LeaseRejected("version_stale")

    @staticmethod
    def _lease_error(reason: str, lease_epoch: int, expected_version: int) -> LeaseConflictError:
        message = _REJECTION_MESSAGES.get(reason, "租约校验未通过")
        return LeaseConflictError(
            message,
            context={"reason": reason, "presented_lease_epoch": lease_epoch, "presented_version": expected_version},
        )

    def _record_rejection(self, task_id: int, worker_id: str, lease_epoch: int, expected_version: int, operation: str, reason: str, now: str) -> None:
        """在独立事务中记录被拒绝的迟到回执，拒绝事件本身绝不因回滚而丢失。"""
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                task = repository.task_by_id(task_id)
                current = None if task is None else {
                    "status": task["status"], "lease_owner": task["lease_owner"],
                    "lease_epoch": task["lease_epoch"], "version": task["version"],
                    "current_result_version": task["current_result_version"],
                }
                repository.add_lease_event(
                    task_id=task_id, event_type="rejected", lease_epoch=int(lease_epoch), actor=worker_id,
                    detail={"operation": operation, "reason": reason, "presented_version": expected_version, "current": current}, now=now,
                )
        except Exception:  # noqa: BLE001 - 审计落痕失败不应掩盖本应返回给调用方的冲突
            pass

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

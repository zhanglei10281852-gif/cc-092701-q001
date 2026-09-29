from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import LeaseConflictError
from app.database import get_connection


TEMPLATE = {
    "code": "grading-a",
    "name": "评分模板",
    "algorithm": "grading-a",
    "parameter_schema": {"rounds": {"type": "integer", "required": True, "minimum": 1, "maximum": 100}},
    "default_parameters": {},
    "max_runtime_seconds": 60,
    "max_attempts": 2,
}


@pytest.fixture()
def service(tmp_path):
    import os

    from app.database import close_connection

    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "fencing.db")
    close_connection()
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 29, 8, 0, 0, tzinfo=UTC))
    instance = ComputeOperationsService(get_connection(), clock)
    instance.create_template(TEMPLATE, "administrator")
    yield instance
    close_connection()


def submit(service: ComputeOperationsService, key: str) -> dict:
    return service.submit(
        {
            "template_code": "grading-a",
            "project_code": "classroom-a",
            "requested_by": "clerk-1",
            "parameters": {"rounds": 3},
            "priority": 50,
            "idempotency_key": key,
        }
    )


def test_late_completion_after_takeover_is_rejected_and_score_does_not_regress(service):
    clock = service.clock
    task = submit(service, "takeover-000001")

    old = service.claim("teacher-old", ["grading-a"], lease_seconds=60)
    assert old["lease_epoch"] == 1
    old_version = old["version"]

    # 旧工作者失联：时钟越过租约边界，恢复器回收，另一台机器接管
    clock.advance(seconds=61)
    assert service.recover_expired() == {"recovered": [task["id"]], "exhausted": []}
    new = service.claim("teacher-new", ["grading-a"], lease_seconds=60)
    assert new["lease_epoch"] == old["lease_epoch"] + 2
    assert new["lease_owner"] == "teacher-new"

    # 旧会话带着过期的代次/版本补交评分：必须得到可区分的冲突，且不写成绩
    with pytest.raises(LeaseConflictError) as exc_info:
        service.complete(
            task["id"], "teacher-old", {"score": 55}, {"seconds": 90},
            lease_epoch=1, expected_version=old_version, receipt_id="late-receipt-01",
        )
    assert exc_info.value.code == "lease_conflict"
    assert exc_info.value.context["reason"] == "owner_changed"
    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["results"] == []
    assert details["current_result_version"] is None

    # 接管者提交最终成绩，完成时间与成绩版本正常推进
    clock.advance(seconds=5)
    succeeded = service.complete(
        task["id"], "teacher-new", {"score": 92}, {"seconds": 40},
        lease_epoch=new["lease_epoch"], expected_version=new["version"], receipt_id="new-receipt-01",
    )
    assert succeeded["status"] == "succeeded"
    assert succeeded["current_result_version"] == 1

    # 旧会话在成绩落定后再次迟到重试，依旧被拒，成绩不倒退
    with pytest.raises(LeaseConflictError) as exc_info:
        service.complete(
            task["id"], "teacher-old", {"score": 55}, {"seconds": 90},
            lease_epoch=1, expected_version=old_version, receipt_id="late-receipt-02",
        )
    assert exc_info.value.context["reason"] in {"owner_changed", "not_running"}
    final = service.get_task(task["id"])
    assert final["lease_owner"] == ""
    assert final["current_result_version"] == 1
    assert final["results"][0]["created_by"] == "teacher-new"
    assert final["results"][0]["result_json"] == '{"score": 92}'

    # 时间线保留每次接管、拒绝与最终成绩，顺序可查
    types = [event["event_type"] for event in final["lease_events"]]
    assert types == ["granted", "recovered", "granted", "rejected", "succeeded", "rejected"]
    rejected = [event for event in final["lease_events"] if event["event_type"] == "rejected"]
    assert {event["actor"] for event in rejected} == {"teacher-old"}
    assert {event["detail_json"] for event in rejected}  # 拒绝原因已落盘


def test_same_worker_id_reclaim_still_fences_old_session_by_epoch(service):
    task = submit(service, "takeover-000002")
    first = service.claim("teacher-w", ["grading-a"], lease_seconds=60)
    service.clock.advance(seconds=61)
    service.recover_expired()
    second = service.claim("teacher-w", ["grading-a"], lease_seconds=60)
    assert second["lease_epoch"] == first["lease_epoch"] + 2

    with pytest.raises(LeaseConflictError) as exc_info:
        service.complete(
            task["id"], "teacher-w", {"score": 40}, {},
            lease_epoch=first["lease_epoch"], expected_version=first["version"], receipt_id="stale-epoch-01",
        )
    # 即使 worker_id 相同，代次失配也必须暴露为旧会话冲突
    assert exc_info.value.context["reason"] == "epoch_stale"


def test_late_failure_report_after_takeover_is_rejected(service):
    task = submit(service, "takeover-000003")
    old = service.claim("teacher-old", ["grading-a"], lease_seconds=60)
    service.clock.advance(seconds=61)
    service.recover_expired()
    new = service.claim("teacher-new", ["grading-a"], lease_seconds=60)

    with pytest.raises(LeaseConflictError) as exc_info:
        service.fail(
            task["id"], "teacher-old", "network_blank", "评分机网络中断", False,
            lease_epoch=1, expected_version=old["version"], receipt_id="late-fail-0001",
        )
    assert exc_info.value.code == "lease_conflict"
    details = service.get_task(task["id"])
    assert details["status"] == "running"
    assert details["lease_owner"] == "teacher-new"
    assert [event["event_type"] for event in details["lease_events"]] == ["granted", "recovered", "granted", "rejected"]


def test_legitimate_completion_retry_is_idempotent_and_not_double_audited(service):
    task = submit(service, "idempotent-00001")
    claimed = service.claim("teacher-1", ["grading-a"], lease_seconds=60)

    def send():
        return service.complete(
            task["id"], "teacher-1", {"score": 78}, {"seconds": 12},
            lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="retry-safe-0001",
        )

    first = send()
    second = send()  # 网络重传：同一回执键、同一内容
    assert first["version"] == second["version"]
    details = service.get_task(task["id"])
    assert len(details["results"]) == 1
    assert details["current_result_version"] == 1
    succeeded_events = [event for event in details["lease_events"] if event["event_type"] == "succeeded"]
    assert len(succeeded_events) == 1
    with get_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM compute_receipts WHERE task_id=?", (task["id"],)).fetchone()[0] == 1


def test_retry_with_same_receipt_id_but_different_payload_is_conflict(service):
    task = submit(service, "idempotent-00002")
    claimed = service.claim("teacher-1", ["grading-a"], lease_seconds=60)
    service.complete(
        task["id"], "teacher-1", {"score": 78}, {},
        lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="retry-safe-0002",
    )
    with pytest.raises(LeaseConflictError) as exc_info:
        service.complete(
            task["id"], "teacher-1", {"score": 79}, {},
            lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="retry-safe-0002",
        )
    assert exc_info.value.context["reason"] == "receipt_conflict"


def test_heartbeat_renews_within_boundary_and_is_rejected_after(service):
    task = submit(service, "heartbeat-00001")
    claimed = service.claim("teacher-1", ["grading-a"], lease_seconds=60)

    service.clock.advance(seconds=59)
    renewed = service.heartbeat(
        task["id"], "teacher-1", lease_seconds=60,
        lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"],
    )
    assert renewed["version"] == claimed["version"] + 1

    # 续期后旧版本号立即失效
    with pytest.raises(LeaseConflictError) as exc_info:
        service.heartbeat(
            task["id"], "teacher-1", lease_seconds=60,
            lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"],
        )
    assert exc_info.value.context["reason"] == "version_stale"

    # 时钟越过新的租约边界：心跳必须被拒，恢复器随后才能接管
    service.clock.advance(seconds=61)
    with pytest.raises(LeaseConflictError) as exc_info:
        service.heartbeat(
            task["id"], "teacher-1", lease_seconds=60,
            lease_epoch=claimed["lease_epoch"], expected_version=renewed["version"],
        )
    assert exc_info.value.context["reason"] == "lease_expired"
    assert service.recover_expired() == {"recovered": [task["id"]], "exhausted": []}


def test_recovered_task_failure_idempotent_retry_replays_stored_snapshot(service):
    task = submit(service, "fail-retry-00001")
    claimed = service.claim("teacher-1", ["grading-a"], lease_seconds=60)
    first = service.fail(
        task["id"], "teacher-1", "numeric_error", "数值不收敛", True,
        lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="fail-retry-same-1",
    )
    # 任务已重新排队、租约结束，迟到但内容相同的合法重传仍返回同一快照，且不重复审计
    replay = service.fail(
        task["id"], "teacher-1", "numeric_error", "数值不收敛", True,
        lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="fail-retry-same-1",
    )
    assert replay["version"] == first["version"]
    with get_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM compute_receipts WHERE task_id=?", (task["id"],)).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM compute_lease_events WHERE task_id=? AND event_type='requeued'", (task["id"],)
        ).fetchone()[0] == 1


def test_takeover_timeline_is_ordered_and_read_back_is_consistent(service):
    clock = service.clock
    task = submit(service, "timeline-000001")
    claimed = service.claim("teacher-old", ["grading-a"], lease_seconds=60)
    clock.advance(seconds=61)
    service.recover_expired()
    reclaimed = service.claim("teacher-new", ["grading-a"], lease_seconds=60)
    with pytest.raises(LeaseConflictError):
        service.complete(
            task["id"], "teacher-old", {"score": 30}, {},
            lease_epoch=claimed["lease_epoch"], expected_version=claimed["version"], receipt_id="timeline-late-1",
        )
    service.complete(
        task["id"], "teacher-new", {"score": 88}, {},
        lease_epoch=reclaimed["lease_epoch"], expected_version=reclaimed["version"], receipt_id="timeline-final-1",
    )

    details = service.get_task(task["id"])
    # 接口读回的状态、工作者身份与成绩版本彼此一致
    assert details["status"] == "succeeded"
    assert details["lease_owner"] == ""
    assert details["lease_epoch"] == reclaimed["lease_epoch"]
    assert details["current_result_version"] == details["results"][-1]["version"] == 1
    assert details["results"][-1]["created_by"] == "teacher-new"

    events = details["lease_events"]
    assert [event["event_type"] for event in events] == ["granted", "recovered", "granted", "rejected", "succeeded"]
    # 时间线严格按发生顺序排列且时间戳单调
    assert [event["id"] for event in events] == sorted(event["id"] for event in events)
    assert [event["created_at"] for event in events] == sorted(event["created_at"] for event in events)
    assert events[0]["lease_epoch"] == 1
    assert events[-2]["lease_epoch"] == 1  # 被拒的是旧代次
    assert events[-1]["lease_epoch"] == reclaimed["lease_epoch"]  # 最终成绩属于新代次

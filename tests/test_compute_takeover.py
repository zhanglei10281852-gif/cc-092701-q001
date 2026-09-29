"""任务接管（lease fencing）回归测试。

场景：实训教室网络中断后，旧教师工作者的迟到回执不得覆盖
已经由另一台机器接管的任务；每次心跳、评分提交和异常回报
都必须证明自己仍持有当前租约与版本。
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import (
    LeaseExpiredError,
    LeaseLostError,
    ReceiptConflictError,
    TaskClosedError,
    VersionConflictError,
)
from app.database import get_connection, init_db


@pytest.fixture(autouse=True)
def isolated_database(tmp_path: Path):
    os.environ["TOWNSHIP_DATABASE_PATH"] = str(tmp_path / "takeover.db")
    from app.database import close_connection
    close_connection()
    yield
    close_connection()

TEMPLATE = {
    "code": "grading-a",
    "name": "实训评分模板",
    "algorithm": "grading-a",
    "parameter_schema": {
        "rubric": {"type": "string", "required": True, "choices": ["v1", "v2"]},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 3,
}


def make_service(hour: int = 8) -> tuple[ComputeOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(datetime(2026, 9, 29, hour, 0, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    return service, clock


def submit(service: ComputeOperationsService, key: str) -> dict:
    return service.submit(
        {
            "template_code": "grading-a",
            "project_code": "training-room-1",
            "requested_by": "student-01",
            "parameters": {"rubric": "v1"},
            "priority": 50,
            "idempotency_key": key,
        }
    )


def event_types(service: ComputeOperationsService, task_id: int) -> list[str]:
    return [event["event_type"] for event in service.get_task(task_id)["lease_events"]]


def test_stale_worker_receipt_cannot_regress_grade_after_takeover():
    """网络恢复后，旧会话迟到的评分回执不得改写新会话的成绩与完成时间。"""
    service, clock = make_service()
    task = submit(service, "takeover-000001")

    old_session = service.claim("teacher-old", ["grading-a"], 10)
    assert old_session["fencing_epoch"] == 1

    # 网络中断：旧工作者失联，租约到期，恢复流程回收任务。
    clock.advance(seconds=11)
    outcome = service.recover_expired()
    assert outcome["recovered"] == [task["id"]]

    # 另一台机器接管任务。
    new_session = service.claim("teacher-new", ["grading-a"], 30)
    assert new_session["lease_token"] != old_session["lease_token"]
    assert new_session["fencing_epoch"] == 3  # 领取、回收、再次领取各推进一次

    # 旧会话恢复网络后提交迟到的评分回执：凭证已失效，必须被可区分地拒绝。
    with pytest.raises(LeaseLostError) as stale:
        service.complete(task["id"], "teacher-old", {"score": 10}, {"note": "late"},
                         old_session["lease_token"], old_session["version"], "receipt-old-1")
    assert stale.value.code == "lease_lost"

    # 新会话正常心跳并提交成绩。
    clock.advance(seconds=5)
    renewed = service.heartbeat(task["id"], "teacher-new", 30,
                                new_session["lease_token"], new_session["version"])
    graded = service.complete(task["id"], "teacher-new", {"score": 95}, {"seconds": 12},
                              renewed["lease_token"], renewed["version"], "receipt-new-1")
    assert graded["status"] == "succeeded"
    finished_at = graded["finished_at"]

    # 旧会话继续重放迟到回执：任务已终结，成绩与完成时间不得倒退。
    with pytest.raises(TaskClosedError):
        service.complete(task["id"], "teacher-old", {"score": 10}, {"note": "late"},
                         old_session["lease_token"], old_session["version"], "receipt-old-1")

    details = service.get_task(task["id"])
    assert details["status"] == "succeeded"
    assert details["current_result_version"] == 1
    assert details["finished_at"] == finished_at
    assert len(details["results"]) == 1
    assert details["results"][0]["created_by"] == "teacher-new"
    assert details["results"][0]["lease_token"] == new_session["lease_token"]
    assert details["consistency"]["consistent"] is True

    # 时间线：接管、拒绝与最终成绩全部按序留痕。
    assert event_types(service, task["id"]) == [
        "lease_granted",      # 旧会话领取
        "lease_reclaimed",    # 租约过期被恢复流程接管
        "lease_granted",      # 新会话接管
        "lease_rejected",     # 旧会话迟到回执被拒（token_mismatch）
        "heartbeat",          # 新会话续租
        "result_accepted",    # 新会话成绩生效
        "lease_rejected",     # 旧会话在终态后继续重放被拒（task_succeeded）
    ]
    rejections = [e for e in details["lease_events"] if e["event_type"] == "lease_rejected"]
    assert rejections[0]["detail"] == "token_mismatch"
    assert rejections[0]["worker_id"] == "teacher-old"
    assert rejections[1]["detail"] == "task_succeeded"


def test_heartbeat_boundary_with_controlled_clock():
    """可控时钟覆盖租约到期边界前后：边界时刻合法，越过边界不得自救。"""
    service, clock = make_service(hour=9)
    task = submit(service, "boundary-000001")
    session = service.claim("teacher-a", ["grading-a"], 10)
    expiry = session["lease_expires_at"]

    # 边界之前一秒：心跳合法。
    clock.advance(seconds=9)
    renewed = service.heartbeat(task["id"], "teacher-a", 10, session["lease_token"], session["version"])
    assert renewed["version"] == session["version"] + 1
    assert renewed["lease_expires_at"] > expiry

    # 恰好到达新的到期时刻：仍然合法（租约严格超过到期时刻才算过期）。
    clock.advance(seconds=10)
    renewed = service.heartbeat(task["id"], "teacher-a", 10, renewed["lease_token"], renewed["version"])
    assert renewed["status"] == "running"

    # 越过边界一秒：凭证本身正确，但租约已过期，旧会话不得靠心跳自救。
    clock.advance(seconds=11)
    with pytest.raises(LeaseExpiredError) as expired:
        service.heartbeat(task["id"], "teacher-a", 10, renewed["lease_token"], renewed["version"])
    assert expired.value.code == "lease_expired"

    # 恢复流程接管后，旧会话的异常回报同样被拒。
    outcome = service.recover_expired()
    assert outcome["recovered"] == [task["id"]]
    with pytest.raises(LeaseLostError):
        service.fail(task["id"], "teacher-a", "network", "连接恢复后补报", True,
                     renewed["lease_token"], renewed["version"])

    details = service.get_task(task["id"])
    rejected = [e["detail"] for e in details["lease_events"] if e["event_type"] == "lease_rejected"]
    assert rejected == ["lease_expired", "task_queued"]


def test_late_receipt_before_and_after_recovery():
    """回执先后顺序：恢复完成前迟到按 lease_expired 拒绝，恢复后按 lease_lost 拒绝。"""
    service, clock = make_service(hour=10)
    task = submit(service, "ordering-000001")
    session = service.claim("teacher-a", ["grading-a"], 10)

    # 租约已过期但恢复流程尚未运行：迟到的评分回执按“租约过期”拒绝。
    clock.advance(seconds=11)
    with pytest.raises(LeaseExpiredError):
        service.complete(task["id"], "teacher-a", {"score": 60}, {},
                         session["lease_token"], session["version"], "receipt-a-1")

    # 恢复流程接管后：同一回执按“租约失效”拒绝，两种冲突结果可区分。
    service.recover_expired()
    with pytest.raises(LeaseLostError):
        service.complete(task["id"], "teacher-a", {"score": 60}, {},
                         session["lease_token"], session["version"], "receipt-a-1")

    details = service.get_task(task["id"])
    rejected = [e["detail"] for e in details["lease_events"] if e["event_type"] == "lease_rejected"]
    assert rejected == ["lease_expired", "task_queued"]
    assert details["status"] == "queued"
    assert details["lease_token"] == ""


def test_idempotent_receipt_retry_does_not_duplicate_audit():
    """合法重试（响应丢失后原样重发）必须返回首次结果，且不产生重复审计。"""
    service, clock = make_service(hour=11)
    task = submit(service, "idempotent-000001")
    session = service.claim("teacher-a", ["grading-a"], 30)

    first = service.complete(task["id"], "teacher-a", {"score": 88}, {"seconds": 5},
                             session["lease_token"], session["version"], "receipt-dup-1")
    assert first["status"] == "succeeded"
    events_before = event_types(service, task["id"])

    # 同一回执键、同一内容重试：返回首次结果，不新增结果版本与审计事件。
    replay = service.complete(task["id"], "teacher-a", {"score": 88}, {"seconds": 5},
                              session["lease_token"], session["version"], "receipt-dup-1")
    assert replay["id"] == first["id"]
    assert replay["current_result_version"] == first["current_result_version"]
    details = service.get_task(task["id"])
    assert len(details["results"]) == 1
    assert event_types(service, task["id"]) == events_before

    # 同一回执键但内容不同：必须按回执冲突拒绝，不能悄悄覆盖。
    with pytest.raises(ReceiptConflictError) as conflict:
        service.complete(task["id"], "teacher-a", {"score": 60}, {"seconds": 5},
                         session["lease_token"], session["version"], "receipt-dup-1")
    assert conflict.value.code == "receipt_conflict"


def test_stale_observed_version_is_rejected():
    """每次提交都必须证明自己持有当前版本：版本陈旧按 version_conflict 拒绝。"""
    service, clock = make_service(hour=12)
    task = submit(service, "version-000001")
    session = service.claim("teacher-a", ["grading-a"], 30)

    clock.advance(seconds=3)
    renewed = service.heartbeat(task["id"], "teacher-a", 30, session["lease_token"], session["version"])

    # 工作者拿着心跳前的旧版本提交：版本已被推进，必须重新读取状态。
    with pytest.raises(VersionConflictError) as stale:
        service.complete(task["id"], "teacher-a", {"score": 70}, {},
                         session["lease_token"], session["version"], "receipt-v-1")
    assert stale.value.code == "version_conflict"
    assert stale.value.context["current_version"] == renewed["version"]

    # 使用最新版本提交则成功。
    graded = service.complete(task["id"], "teacher-a", {"score": 70}, {},
                              renewed["lease_token"], renewed["version"], "receipt-v-1")
    assert graded["status"] == "succeeded"


def test_same_worker_id_new_session_requires_new_token():
    """同一台教师机重启后重新接管：旧会话凭证即使 worker_id 相同也必须失效。"""
    service, clock = make_service(hour=13)
    task = submit(service, "restart-000001")
    first_session = service.claim("teacher-shared", ["grading-a"], 10)

    clock.advance(seconds=11)
    service.recover_expired()
    second_session = service.claim("teacher-shared", ["grading-a"], 30)
    assert second_session["lease_token"] != first_session["lease_token"]

    # 旧进程恢复后仍用第一份凭证提交：必须被拒，成绩只能由新会话写入。
    with pytest.raises(LeaseLostError):
        service.complete(task["id"], "teacher-shared", {"score": 1}, {},
                         first_session["lease_token"], first_session["version"], "receipt-old-proc")
    graded = service.complete(task["id"], "teacher-shared", {"score": 92}, {},
                              second_session["lease_token"], second_session["version"], "receipt-new-proc")
    assert graded["status"] == "succeeded"
    details = service.get_task(task["id"])
    assert details["results"][0]["fencing_epoch"] == second_session["fencing_epoch"]
    assert details["consistency"]["consistent"] is True


def test_readback_consistency_across_takeover(client):
    """接口读回：任务状态、工作者身份和成绩版本在每次接管前后彼此一致。"""
    client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    created = client.post(
        "/api/compute/tasks",
        json={
            "template_code": "grading-a",
            "project_code": "training-room-1",
            "requested_by": "student-02",
            "parameters": {"rubric": "v2"},
            "priority": 60,
            "idempotency_key": "readback-000001",
        },
    )
    assert created.status_code == 202
    task_id = created.json()["id"]

    claimed = client.post("/api/compute/tasks/claim",
                          json={"worker_id": "teacher-a", "capabilities": ["grading-a"], "lease_seconds": 60})
    session = claimed.json()["task"]
    running = client.get(f"/api/compute/task-details/{task_id}").json()
    assert running["status"] == "running"
    assert running["lease_owner"] == "teacher-a"
    assert running["lease_token"] == session["lease_token"]
    assert running["consistency"]["consistent"] is True

    # 缺少租约凭证的回执在接口层即被拒绝（参数校验）。
    missing = client.post(f"/api/compute/tasks/{task_id}/complete",
                          json={"worker_id": "teacher-a", "result": {"score": 1}})
    assert missing.status_code == 422

    # 伪造凭证：可区分的 409 lease_lost。
    forged = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "teacher-a", "lease_token": "forged-token", "observed_version": session["version"],
              "result": {"score": 1}, "receipt_key": "receipt-forged"},
    )
    assert forged.status_code == 409
    assert forged.json()["error"]["code"] == "lease_lost"

    completed = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "teacher-a", "lease_token": session["lease_token"],
              "observed_version": session["version"], "result": {"score": 97},
              "metrics": {"seconds": 3}, "receipt_key": "receipt-final-1"},
    )
    assert completed.status_code == 200

    final = client.get(f"/api/compute/task-details/{task_id}").json()
    assert final["status"] == "succeeded"
    assert final["lease_owner"] == "" and final["lease_token"] == ""
    assert final["current_result_version"] == 1
    assert final["results"][0]["version"] == final["current_result_version"]
    assert final["results"][0]["created_by"] == "teacher-a"
    assert final["consistency"]["consistent"] is True

    # 终态后任何迟到回执都得到可区分的 task_closed。
    late = client.post(
        f"/api/compute/tasks/{task_id}/fail",
        json={"worker_id": "teacher-a", "lease_token": session["lease_token"],
              "observed_version": session["version"], "error_code": "late", "message": "迟到异常"},
    )
    assert late.status_code == 409
    assert late.json()["error"]["code"] == "task_closed"

    # 合法重试：同一回执键原样重发，返回首次结果且状态不变。
    retry = client.post(
        f"/api/compute/tasks/{task_id}/complete",
        json={"worker_id": "teacher-a", "lease_token": session["lease_token"],
              "observed_version": session["version"], "result": {"score": 97},
              "metrics": {"seconds": 3}, "receipt_key": "receipt-final-1"},
    )
    assert retry.status_code == 200
    assert retry.json()["current_result_version"] == 1
    again = client.get(f"/api/compute/task-details/{task_id}").json()
    assert len(again["results"]) == 1

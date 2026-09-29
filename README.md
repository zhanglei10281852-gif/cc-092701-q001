# 职业教学任务运营服务

这是一个面向职业院校教务团队、授课教师和课程管理员的 Python 后端服务，用于管理课程任务模板、学员提交、执行队列、教师工作者、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

课程任务运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。

## 任务接管与租约栅栏

为防止网络短暂中断后失联工作者把迟到的评分/异常回执写回已被接管的任务，领取与回执遵循栅栏令牌（fencing token）协议：

- 每次领取（`POST /api/compute/tasks/claim`）返回单调递增的 `lease_epoch`（栅栏令牌）与任务聚合 `version`。租约被恢复器吊销和任务被再次领取都会推进 `lease_epoch`，因此同一 `worker_id` 的新会话也无法与旧会话混淆。
- 心跳（`heartbeat`）、评分提交（`complete`）和异常回报（`fail`）必须同时出示 `lease_epoch` 与 `expected_version`，且租约未过期。服务端在即时事务内做持有者、代次、过期边界与版本的四重校验，并以条件更新（CAS）兜底。
- 校验失败统一返回 `409 lease_conflict`，并在 `error.context.reason` 中给出可区分原因：`owner_changed`（已被他人接管）、`epoch_stale`（接管前的旧会话）、`lease_expired`（租约过期）、`version_stale`（版本落后）、`not_running`（任务已终态）、`receipt_conflict`（回执键被用于不同内容）。被拒写入不会改动成绩与完成时间。
- 回执必须带 `receipt_id` 幂等键：同代次、同键、同内容的合法重传直接返回首次响应的快照，不重复插入成绩版本，也不重复写入时间线；键相同但内容不同则判为冲突。
- `compute_lease_events` 按发生顺序保留每次授予（`granted`）、续期（`renewed`）、恢复（`recovered`）、拒绝（`rejected`）、成功（`succeeded`）、失败（`failed`）与重排队（`requeued`）；拒绝事件在独立事务中落盘，主业务回滚也不会丢失。任务详情接口（`/api/compute/task-details/{id}`）读回的 `status`、`lease_owner`、`lease_epoch`、`current_result_version` 与成绩列表、租约事件时间线彼此一致。
- 恢复器（`POST /api/compute/recovery/expired-leases`）只回收 `lease_expires_at` 已过界的运行中任务，并在回收时即轮换 `lease_epoch`，使重新排队窗口内的僵尸会话同样失效。时钟通过 `Clock`/`FrozenClock` 注入，可精确覆盖过期边界与回执先后顺序。


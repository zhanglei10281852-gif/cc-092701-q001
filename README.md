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

### 任务接管与租约凭证

任务被领取时服务端签发随机 `lease_token` 并递增 `fencing_epoch`，工作者必须在后续每次心跳、评分提交（complete）和异常回报（fail）中同时出示：

- `worker_id` + `lease_token`：证明自己持有当前租约，而不是同名旧进程；
- `observed_version`：证明自己看到的是当前任务版本；
- 租约到期判定使用服务端可控时钟，过期后旧会话无法用心跳自救。

租约过期由恢复流程回收：清空令牌、再次推进 `fencing_epoch`、任务回到队列（或按尝试次数终态失败）。旧会话恢复网络后的迟到回执会得到可区分的冲突：

| HTTP | code | 含义 |
| --- | --- | --- |
| 409 | `lease_expired` | 凭证正确但租约已过期，恢复流程尚未接管 |
| 409 | `lease_lost` | 令牌已失效（任务被接管或已回队列） |
| 409 | `version_conflict` | 租约有效但任务版本已推进，需重新读取 |
| 409 | `task_closed` | 任务已进入 succeeded/failed/cancelled 终态 |
| 409 | `receipt_conflict` | 回执键被复用于不同工作者或不同内容 |

评分提交携带 `receipt_key`：响应丢失后的合法重试原样返回首次结果，不产生第二个结果版本、不重复审计；同键不同内容（或属于已被重试取代的旧尝试）按 `receipt_conflict` 拒绝。每次领取（`lease_granted`）、续租（`heartbeat`）、接管回收（`lease_reclaimed`）、拒绝（`lease_rejected`）与成绩/异常受理（`result_accepted`/`failure_accepted`）都写入 `compute_lease_events` 时间线。任务详情接口返回 `consistency` 字段，交叉核对任务状态、工作者身份与成绩版本是否彼此一致。

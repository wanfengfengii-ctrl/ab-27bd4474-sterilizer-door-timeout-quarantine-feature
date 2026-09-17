# CSSD 灭菌柜卸载窗口裁决服务

面向消毒供应中心（CSSD）设备集成的纯后端服务。灭菌柜结束程序后，设备必须在
限定窗口（默认 30 秒，可按设备配置 1–600 秒）内完成开门确认；确认请求与
超时扫描几乎同时到达时，系统对同一窗口只形成一个稳定的终态：`confirmed`
（正常卸载）或 `quarantined`（隔离）。

## 架构

两个应用进程共享同一个 SQLite 数据库文件，**数据库是两进程的唯一裁决依据**：

```
                ┌─────────────┐
  设备/集成方 ──▶│  api (Gin)  │──┐  登记窗口 / 开门确认 / 策略更新 / 查询
                └─────────────┘  │        ┌──────────────────┐
                                 ├───────▶│  SQLite (WAL)    │
                ┌─────────────┐  │        │  devices         │
                │   worker    │──┘        │  device_policies │
                └─────────────┘           │  unload_windows  │
                 周期扫描到期窗口            └──────────────────┘
```

- `cmd/api`：HTTP API，登记卸载窗口、接收开门确认、更新卸载时限策略、查询状态。
- `cmd/worker`：独立扫描进程，把到期未确认的 `open` 窗口写入 `quarantined`。
- `cmd/verify`：一次性验收客户端（见下文「验收」）。
- `internal/store`：SQLite 持久化层（`database/sql` + `modernc.org/sqlite`，纯 Go，无 CGO）。
- `internal/api`：Gin HTTP 层，只做参数校验、错误映射与序列化，裁决全部下沉到 SQL。

### 状态机

```
                 数据库 UTC now <  deadline        ┌─────────────┐
              ┌──────────────────────────────────▶ │  confirmed  │ 终态
   ┌───────┐  │  （开门确认到达）                    └─────────────┘
   │ open  │──┤
   └───────┘  │  数据库 UTC now >= deadline        ┌─────────────┐
              ├──────────────────────────────────▶ │ quarantined │ 终态
                 （确认迟到 confirm_after_deadline   └─────────────┘
                   或扫描超时 scan_timeout）
```

`open` 是唯一非终态；终态之后不存在任何迁移。

## UTC 时间边界规则

所有参与裁决的时刻**一律取自数据库自身的 UTC 当前时间**，客户端与应用进程
的时钟不参与裁决：

- 时间由 SQLite 的 `strftime('%Y-%m-%dT%H:%M:%fZ','now')` 生成，UTC、毫秒
  精度，形如 `2026-09-15T00:19:26.197Z`；定长格式下字符串字典序即时间序。
- **登记**：`deadline = 数据库 UTC now + 设备当前策略秒数（默认 30 秒）`，
  与 `opened_at`、`ttl_seconds` 快照在同一条 INSERT 内生成。客户端不得
  指定截止时刻——登记/确认接口拒绝任何请求字段（返回 `400 INVALID_REQUEST`）。
- **确认**：单条 UPDATE 内取数据库时间 `now`：
  - `now < deadline`（严格早于）→ 写入 `confirmed`，原因 `door_open_confirmed`；
  - `now >= deadline`（等于或晚于）→ 写入 `quarantined`，原因 `confirm_after_deadline`。
- **扫描**：worker 把满足 `now >= deadline` 的 `open` 窗口写入 `quarantined`，
  原因 `scan_timeout`——与确认使用**完全相同的边界**，等号恒归属隔离侧。
- SQLite 保证同一语句内多次调用 `'now'` 返回完全相同的值，因此同一条
  UPDATE 里的边界判定与 `closed_at` 使用的是同一个数据库时刻。

## 卸载时限策略

消毒供应中心可按设备型号设定不同的开门确认时限：

- **更新**：`PUT /devices/{id}/policy`，请求体 `{"ttl_seconds": 45}`，取值
  为 1–600 秒的整数。更新是单条原子 UPSERT：要么完整生效，要么整体失败，
  不会留下半写入配置，也不会改动设备已登记的任何窗口。值缺失、非整数或
  越界返回 `400 INVALID_REQUEST`（`details` 携带 `field`/`min`/`max`）；
  设备不存在返回 `404 DEVICE_NOT_FOUND`。
- **生效范围**：策略只在登记窗口时被读取。登记由同一条 INSERT（隐式事务）
  完成：读取设备当前策略（未配置时回落默认 30 秒）→ 生成截止时刻 → 把
  采用的秒数快照到窗口的 `ttl_seconds` 列。策略的后续变化**不回改**已登记
  窗口；活动窗口仍按其登记时的快照时限裁决。
- **可解释性**：`GET /devices/{id}/status` 同时返回当前生效的 `policy`
  （含 `source`：`default` 未配置 / `custom` 已配置）与最近窗口的
  `ttl_seconds` 快照，调用方可据此解释每个截止时刻的来源。
- **裁决不变**：窗口的确认与超时扫描仍按原有数据库 UTC 边界和
  `state='open'` 前置条件裁决，与策略机制正交。
- **迁移**：既有数据库在进程启动时自动迁移——`unload_windows` 增加
  `ttl_seconds` 列（存量窗口回填默认 30 秒），并新建 `device_policies`
  表；无需手工干预。

## 并发裁决保证

- 确认与扫描的终态写入都是**单条、带 `WHERE state='open'` 前置条件的原子
  UPDATE**。SQLite 将两条写语句串行化，只有先执行的一条能命中目标行；
  后到者命中 0 行，确认侧据此返回 `409 NO_OPEN_WINDOW`。因此无论竞争顺序
  如何，同一窗口只会提交一个终态。
- 同一设备至多一个 `open` 窗口，由数据库**部分唯一索引**
  `ux_unload_windows_open_device` 强制保证；并发登记时第二个 INSERT 必然
  因唯一约束失败，映射为 `409 WINDOW_ALREADY_OPEN`。
- 两个进程均以 `busy_timeout=5000` + WAL 打开同一数据库文件：写冲突在驱动
  层自动重试，读写可并发。

## HTTP API

基础地址：`http://localhost:8080`（可用 `API_PORT` 覆盖宿主端口）。

| 方法 | 路径 | 说明 | 成功 | 主要错误 |
|------|------|------|------|----------|
| GET | `/health` | 健康检查（含数据库连通性） | 200 | 503 |
| POST | `/devices` | 登记设备 `{device_id, name}` | 201 | 409 `DEVICE_ALREADY_EXISTS` |
| PUT | `/devices/{id}/policy` | 更新卸载时限策略 `{ttl_seconds}`（1–600 整数） | 200 | 404 `DEVICE_NOT_FOUND`、400 `INVALID_REQUEST` |
| POST | `/devices/{id}/windows` | 登记卸载窗口（空请求体或 `{}`） | 201 | 404 `DEVICE_NOT_FOUND`、409 `WINDOW_ALREADY_OPEN`、400 `INVALID_REQUEST` |
| POST | `/devices/{id}/confirm` | 开门确认（空请求体或 `{}`） | 200 | 404 `DEVICE_NOT_FOUND`、409 `NO_OPEN_WINDOW` |
| GET | `/devices/{id}/status` | 查询设备、当前策略及最近窗口（无窗口时 `window` 为 `null`） | 200 | 404 `DEVICE_NOT_FOUND` |

窗口对象（`ttl_seconds` 为登记时采用的时限快照）：

```json
{
  "id": 3,
  "device_id": "STER-DEMO",
  "state": "open",
  "opened_at": "2026-09-15T00:19:26.197Z",
  "deadline":  "2026-09-15T00:19:56.197Z",
  "closed_at": null,
  "close_reason": null,
  "ttl_seconds": 30
}
```

策略对象（随策略更新与状态查询返回）：

```json
{
  "device_id": "STER-DEMO",
  "ttl_seconds": 30,
  "source": "default",
  "updated_at": null
}
```

错误一律为结构化响应：

```json
{
  "error": {
    "code": "WINDOW_ALREADY_OPEN",
    "message": "device \"STER-DEMO\" already has an open unload window",
    "details": { "deadline": "2026-09-15T00:19:56.197Z", "open_window_id": 3 }
  }
}
```

### 示例

```bash
curl -X POST localhost:8080/devices -H 'Content-Type: application/json' \
     -d '{"device_id":"STER-01","name":"灭菌柜 1"}'

curl -X PUT localhost:8080/devices/STER-01/policy -H 'Content-Type: application/json' \
     -d '{"ttl_seconds":45}'                               # 200，该设备改用 45 秒时限

curl -X POST localhost:8080/devices/STER-01/windows          # 201，deadline 由数据库生成
curl -X POST localhost:8080/devices/STER-01/confirm          # 200，confirmed 或 quarantined
curl     localhost:8080/devices/STER-01/status               # 查询当前策略与最终状态
```

## 配置

| 变量 | 组件 | 默认值 | 说明 |
|------|------|--------|------|
| `DATABASE_PATH` | api / worker | `cssd.db`（容器内 `/data/cssd.db`） | SQLite 文件路径，两进程必须一致 |
| `PORT` | api | `8080` | 容器内监听端口 |
| `API_PORT` | compose | `8080` | 映射到宿主的端口 |
| `SCAN_INTERVAL` | worker | `1s` | 扫描周期（Go duration，如 `500ms`） |
| `API_BASE_URL` | verify | `http://localhost:8080` | 验收目标地址 |

## 本地运行与测试

需要 Go 1.25。

```bash
go test ./...            # 单元/并发裁决测试（可加 -race）
go run ./cmd/api &       # 启动 API（:8080）
go run ./cmd/worker &    # 启动扫描 worker
```

测试覆盖：及时确认、无人确认超时隔离、数据库时间恰好位于边界时确认与扫描
的并发裁决（恰好一方提交终态且恒为 quarantined）、活动窗口冲突（含并发
登记恰好一个成功）、策略快照语义（默认 30 秒、更新后新窗口采用新时限、
活动窗口不受再次改策影响、越界/缺失值被拒绝且不留半写入）、既有数据库
的列迁移，并通过查询断言唯一最终状态。

## Docker Compose 运行

```bash
docker compose up --build          # 仅启动 api 与 worker 两个应用组件
API_PORT=9090 docker compose up    # 覆盖宿主端口
```

### 验收（一次性 verify 服务）

`verify` 位于独立 profile，不影响默认启动。它会等待 API 健康后执行完整
验收（及时确认与默认 30 秒窗口、30 秒超时隔离、活动窗口冲突、客户端禁止
指定截止时刻、策略参数校验与快照语义、终态后重复确认），全部通过输出
`VERIFY: PASS` 并以退出码 0 结束：

```bash
docker compose --profile verify up --build --abort-on-container-exit --exit-code-from verify
docker compose down                # 清理（保留数据卷；加 -v 一并删除）
```

## 目录结构

```
cmd/api/main.go        HTTP API 进程入口
cmd/worker/main.go     到期扫描进程入口
cmd/verify/main.go     一次性验收客户端
internal/api/          Gin 路由、参数校验、结构化错误
internal/store/        SQLite 持久化与全部裁决 SQL（含并发测试）
Dockerfile             单一镜像构建三个二进制
docker-compose.yml     api + worker（默认）与 verify（profile）
```

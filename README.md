# 邮件归档整理服务（FastAPI）

批量提交邮件头信息（`message_id`、`in_reply_to`、`references`、发送时间、主题），
服务将其归并为**稳定排序的会话树**，并给出**重复 ID、缺失引用、引用环**三类诊断。
批次请求与归并结果均持久化到 SQLite，可随时查询。

## 一条命令启动

需要 Docker 20+ 与 Docker Compose v2：

```bash
docker compose up --build
```

启动后服务监听 **8000** 端口：

| 地址 | 说明 |
| --- | --- |
| <http://localhost:8000> | 服务根 |
| <http://localhost:8000/docs> | Swagger UI（可直接发请求调试） |
| <http://localhost:8000/health> | 健康检查，返回 `{"status":"ok"}` |
| <http://localhost:8000/openapi.json> | OpenAPI 描述 |

首次启动会自动在卷内安装环境就绪后的建表全部完成（`app.main` lifespan 中执行幂等 DDL）；
数据库存放在命名卷 `mailarchive-data` 中，容器重建数据不丢。

## 必要配置

均通过环境变量注入，均有默认值：

| 变量 | 默认值（本地）/ Compose 值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `data/mailarchive.db` / `/data/mailarchive.db` | SQLite 数据库文件路径，父目录不存在会自动创建，表自动建立 |

端口通过 Compose 的 `8000:8000` 映射发布；如需换宿主机端口，例如改成本地 9000：

```yaml
ports:
  - "9000:8000"
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 提交一批邮件，返回会话树与全部诊断（HTTP 201） |
| `GET` | `/batches?limit=50&offset=0` | 批次列表，按提交时间倒序 |
| `GET` | `/batches/{batch_id}` | 取该批次的归档结果快照（与提交响应完全一致） |
| `POST` | `/relinks` | 迟到邮件归档后，批量修复此前断链的有效邮件（HTTP 200） |
| `GET` | `/messages/{message_id}` | 查询单封邮件及其最终父节点 |
| `GET` | `/threads/{message_id}` | 查询该邮件所属完整会话树；环邮件返回 **409** |
| `GET` | `/threads/{message_id}/page?limit=&cursor=` | 会话树**游标分页**（先序扁平展开，含 `depth`），适配大型会话 |
| `GET` | `/health` | 健康检查 |

未知 ID：批次 / 邮件查询、`/relinks` 返回 **404**；请求体不合法返回 **422**；
`/relinks` 若合并候选关系后形成环，整次返回 **409** 且不写库。

### 大型会话分页：`GET /threads/{message_id}/page`

完整树 `GET /threads/{message_id}` 行为保持不变；需要遍历大型会话时使用分页接口：

- 首次请求**不带** `cursor`，沿用与完整树完全一致的子节点排序
  （`(sent_at, message_id)` 升序）做**先序展开**；
- 返回扁平 `items`（节点字段与树节点相同，去掉 `children`、增加整数
  `depth`，根为 0）、`root_id`、`limit` 与不透明 `next_cursor`；
- `limit` 限定 **1～200**（默认 50）；取到末页时 `next_cursor` 为 `null`；
- 游标封装**根 ID、下一偏移与 revision**：revision 根据整棵树每个节点的
  `message_id` / `parent_id` / `sent_at` 计算。后续请求**必须重新计算
  revision**——若会话树在此期间被补链（`POST /relinks`）或新邮件并入而
  发生变化，返回 **409** `{"detail": {"error": "cursor_stale", …}}`，
  调用方应丢弃游标、从第一页重新开始，从而避免跨页漏项或重复；
- 游标格式错误（无法解析、字段缺失/非法、版本不符）返回 **422**；
  游标来自**另一棵会话树**（与路径 `message_id` 当前所属会话不符）同样
  返回 **422**；路径邮件未知返回 **404**；环内邮件返回 **409**。
- 游标为不透明字符串，客户端不应自行解析或构造。

```bash
# 服务监听 8000 端口；首页（无需 cursor）
curl 'http://localhost:8000/threads/A/page?limit=2'
```

```jsonc
{
  "root_id": "A",
  "limit": 2,
  "items": [
    {"message_id": "A", "parent_id": null, "depth": 0,
     "in_reply_to": null, "references": [],
     "sent_at": "2026-09-18T08:00:00+00:00", "subject": "例会通知"},
    {"message_id": "B", "parent_id": "A", "depth": 1,
     "in_reply_to": "A", "references": ["A"],
     "sent_at": "2026-09-18T09:00:00+00:00", "subject": "Re: 例会通知"}
  ],
  "next_cursor": "djEcxY…"          // 原样回传取下一页；末页为 null
}
```

```bash
# 翻页：把上一页的 next_cursor 原样带上
curl 'http://localhost:8000/threads/A/page?limit=2&cursor=djEcxY%E2%80%A6'
```

补链后继续使用旧游标（HTTP **409**，树已变化）：

```json
{
  "detail": {
    "error": "cursor_stale",
    "message": "the thread has changed since the cursor was issued; restart from the first page"
  }
}
```

### 提交示例

```bash
curl -X POST http://localhost:8000/batches \
  -H 'Content-Type: application/json' \
  -d '{
    "messages": [
      {"message_id": "A", "in_reply_to": null, "references": [],
       "sent_at": "2026-09-18T08:00:00Z", "subject": "例会通知"},
      {"message_id": "B", "in_reply_to": "A", "references": ["A"],
       "sent_at": "2026-09-18T09:00:00Z", "subject": "Re: 例会通知"},
      {"message_id": "C", "in_reply_to": "B", "references": ["A", "B"],
       "sent_at": "2026-09-18T10:00:00Z", "subject": "Re: 例会通知"},
      {"message_id": "D", "in_reply_to": "lost-123", "references": ["ghost-9"],
       "sent_at": "2026-09-18T11:00:00Z", "subject": "断链邮件"},
      {"message_id": "X", "in_reply_to": "Y", "references": ["Y"],
       "sent_at": "2026-09-18T12:00:00Z", "subject": "环一"},
      {"message_id": "Y", "in_reply_to": "X", "references": ["X"],
       "sent_at": "2026-09-18T13:00:00Z", "subject": "环二"}
    ]
  }'
```

### 响应结构

```jsonc
{
  "batch_id": "…",                 // 批次主键，查询接口据此取快照
  "submitted_at": "…UTC ISO8601",
  "totals": {                      // 计数：提交数 / 新唯一数 / 历史重复 / 环内无效 / 树数 …
    "submitted": 6, "unique_new": 6, "archived_duplicates": 0,
    "invalid": 2, "missing_reference_messages": 2, "cycles": 1, "trees": 2
  },
  "threads": [                     // 稳定排序的会话树（见下）
    {
      "message_id": "A",
      "parent_id": null,          // 根节点
      "in_reply_to": null,
      "references": [],
      "sent_at": "2026-09-18T08:00:00+00:00",
      "subject": "例会通知",
      "children": [ { "message_id": "B", "parent_id": "A", "children": [ … ] } ]
    }
  ],
  "duplicates": [                  // 重复 message_id（本批重复 + 历史归档重复）
    {"message_id": "Q", "archived": true,  "first_submitted_seq": null, "occurrences": [2]}
  ],
  "missing_references": [          // 每封邮件引用中、已知集合里不存在的去重 ID
    {"message_id": "D", "missing": ["ghost-9", "lost-123"]}
  ],
  "cycles": [                      // 引用环：整环成员（环序，环内最小 ID 起头）
    {"index": 1, "members": ["X", "Y"], "invalid_messages": ["X", "Y"]}
  ],
  "invalid_messages": [            // 被判 invalid 的邮件明细及原因
    {"message_id": "X", "reason": "reference_cycle", "cycle_index": 1, "…": "…"}
  ]
}
```

### 迟到邮件补链：`POST /relinks`

邮件归档时若 `in_reply_to` / `references` 指向的邮件尚未到达，该邮件会断链自成根。
等迟到邮件在后续批次归档后，可用本接口按**原父节点规则**批量修复这些断链：

- 请求体给出待处理 `message_ids`（自动去除空白项、去重并保持顺序；空列表 / 全空白返回 **422**）；
- 每封邮件**仅在当前 `parent_id` 为空**（断链根、且非引用环内 `invalid` 邮件）时处理：
  1. 先取现已存在且有效的 `in_reply_to`；
  2. 否则从 `references` 右向左回退，取最后一个已知项；
  3. 仍无法命中、或邮件本就挂接在树下、或属于引用环，则**保持不变**；
- 所有候选关系会**合并到既有父指针图上统一检测环**：任一变更形成环（含自指）则整次
  返回 **409**（响应体给出环成员），**不写任何库**；
- 无环时在**单事务**内更新 `parent_id`，并按请求顺序返回每封邮件的
  `old_parent_id` / `new_parent_id` / `old_root_id` / `new_root_id`（未变更者新旧值相同），
  `relinked` 为实际发生变更的计数；不存在的 ID 返回 **404** 且不写库。

补链成功后 `GET /threads`、`GET /messages` 随即反映新树；**既有批次结果快照
（`GET /batches/{id}`）与诊断明细保持归档时原样，不做改写。**

```bash
# 1) D 归档时其父 late 尚未到达 -> D 断链为根
curl -X POST http://localhost:8000/batches -H 'Content-Type: application/json' -d '{
  "messages": [
    {"message_id": "D", "in_reply_to": "late", "references": ["ghost"],
     "sent_at": "2026-09-18T09:00:00Z", "subject": "断链邮件"}
  ]
}'

# 2) 迟到邮件归档
curl -X POST http://localhost:8000/batches -H 'Content-Type: application/json' -d '{
  "messages": [
    {"message_id": "late", "in_reply_to": null, "references": [],
     "sent_at": "2026-09-18T07:00:00Z", "subject": "迟到的原邮件"}
  ]
}'

# 3) 修复断链
curl -X POST http://localhost:8000/relinks -H 'Content-Type: application/json' \
  -d '{"message_ids": ["D"]}'
```

```jsonc
{
  "relinked": 1,
  "results": [
    {
      "message_id": "D",
      "old_parent_id": null,
      "new_parent_id": "late",   // 命中现已存在的 in_reply_to
      "old_root_id": "D",
      "new_root_id": "late"
    }
  ]
}
```

成环时（HTTP **409**，不写库）：

```json
{
  "detail": {
    "message": "relink would create a reference cycle; no changes were written",
    "cycles": [["p", "q"]]
  }
}
```

## 归并规则

- **父节点确定**
  1. 优先采用有效的 `in_reply_to`：指向已知邮件（本批或历史已归档）即采用；
  2. 否则从右向左扫描 `references`，取其中**最后一个已知项**；
  3. 都命不中（含 `in_reply_to`/`references` 指向不存在的 ID）即为断链邮件，**自成根节点**。
- **引用环**：候选父子关系构成功能图（每封新邮件至多一个候选父）。
  检测到环时，**环上所有邮件整体标记为 `invalid`，绝不部分挂接**（不会下沉成根、
  不会挂到其它树）；随后引用了该环的邮件会回到 `references` 中重新寻找其它有效父节点，
  重复检测直至无环。自引用（`in_reply_to` 指向自身）视为单节点环。
- **重复 ID**：同批次重复出现时仅采用首次出现的内容参与归并；若 ID 已在历史批次归档，
  本次不重复落库。两种情况都会在 `duplicates` 中标注。
- **稳定排序**：根之间、同一父节点的子节点之间统一按
  `(sent_at UTC, message_id)` 升序，先序输出，结果全序确定、可复现。
- **跨批次**：历史邮件是合法的父节点。归档时历史父指针不可变，因此引用环只可能完全位于
  同一批新邮件内部；归档后迟到邮件可经 `POST /relinks` 修复断链根（见上），修复前会把全部
  候选关系合并到既有图上统一检测环，绝不允许写后成环。
- **时间**：`sent_at` 无时区时按 UTC 处理，有偏移时换算为 UTC 存储与排序。

## 持久化

SQLite（WAL 模式），表在服务启动时自动创建：

- `batches`：批次元数据与计数；
- `messages`：每封唯一邮件、最终 `parent_id`、是否 `is_invalid`；
- `batch_duplicates` / `batch_missing_refs` / `batch_cycles`：诊断明细；
- `batch_results`：完整结果 JSON 快照，`GET /batches/{id}` 原样返回（补链只更新
  `messages.parent_id`，快照与诊断表永不改写）。

写操作在进程内串行化（线程锁 + 单连接写事务），保证并发提交 / 补链下的归并一致性。

## 本地开发（不使用 Docker）

需要 Python 3.11+：

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
# 服务监听 8000 端口：http://localhost:8000  （/docs 为 Swagger UI；
# DATABASE_PATH 默认 data/mailarchive.db）
```

运行测试：

```bash
pip install pytest
python -m pytest -q
```

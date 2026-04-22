# hycs_task

Telegram 客服任务机器人（独立仓库，不影响原 hybot）。

## 已实现（v2）

- 监听指定客服群 `SUPPORT_GROUP_IDS`
- 自动筛选“疑似问题消息”并建任务（含噪音过滤）
- 汇总推送到任务频道 `TASK_CHANNEL_ID`
- 任务超时计时（默认 SLA 10 分钟）+ 提醒（每任务最多 3 次）
- 任务状态流转：`OPEN -> PROCESSING -> DONE`
  - `/task_claim <id>` 认领
  - `/task_open <id>` 回退 OPEN
  - `/task_done <id>` 完成
- 任务卡片自动更新（状态/处理人/绑定订单）
- **v3 闭环回传**：在任务频道回复任务卡片，机器人会自动把该回复回传到原客服群对应问题下
  - 支持文本/图片/文件等消息（copy_message）
  - 支持在消息里带 `#任务ID` 直接指定任务
  - 回传成功后自动把 OPEN 任务置为 PROCESSING（并写入处理人）
- 手动建任务：`/task_new`（回复消息触发）
- 订单关联：
  - `/task_bind <id> <order_no>`
  - `/task_order <id>`
- 快捷语库：
  - `/qr_add <key> <text>`
  - `/qr_list`
  - `/qr <key>`
- 本地任务存储：SQLite (`tasks.db`)
- 订单查询：PostgreSQL（按 `order_no/mch_order_no`）

## 环境变量

```bash
cp .env.example .env
```

关键参数：

- `BOT_TOKEN`
- `SUPPORT_GROUP_IDS`
- `TASK_CHANNEL_ID=-1003892387186`
- `ADMIN_TG_IDS`
- `TASK_SLA_MINUTES=10`
- `QUESTION_KEYWORDS` / `NOISE_KEYWORDS`
- `ORDER_API_URL` / `ORDER_API_TOKEN`（推荐，走 hybot 内部 API）
- PostgreSQL 参数（备选，如需 `/task_order` 直连DB）

## 运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a; source .env; set +a
python3 bot.py
```

## 方案B：hybot 内部查单接口（推荐）

`hycs_task` 已支持优先调用内部 API 查单：

- `ORDER_API_URL` 例如：`http://127.0.0.1:8081/internal/order/query`
- `ORDER_API_TOKEN` Bearer Token（可选但推荐）

请求：

```json
{"order_token":"xxx"}
```

响应可用两种格式（任选其一）：

```json
{"ok":true,"data":{"order_no":"...","mch_order_no":"...","status":"..."}}
```

或直接：

```json
{"order_no":"...","mch_order_no":"...","status":"..."}
```

## 说明

- 本项目在新仓库开发，不改动原 `hybot`。
- 自动任务筛选逻辑在 `looks_like_question()`，可继续按业务优化。
- 若任务频道为私有 channel，确保 bot 已加入并具备发言/编辑消息权限。

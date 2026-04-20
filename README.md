# hycs_task

Telegram 客服任务机器人（独立仓库，不影响原 hybot）。

## 已实现（MVP）

- 监听指定客服群 `SUPPORT_GROUP_IDS`
- 自动筛选“疑似问题消息”并建任务
- 汇总推送到任务频道 `TASK_CHANNEL_ID`
- 任务超时计时（默认 SLA 10 分钟）+ 分级提醒（每任务最多 3 次）
- 任务命令：
  - `/task_list`
  - `/task_done <task_id>`
  - `/task_bind <task_id> <order_no>`
  - `/task_order <task_id>`
- 快捷语库：
  - `/qr_add <key> <text>`
  - `/qr_list`
  - `/qr <key>`
- 本地任务存储：SQLite (`tasks.db`)
- 订单查询：PostgreSQL（按 `order_no/mch_order_no`）

## 环境变量

复制并编辑：

```bash
cp .env.example .env
```

关键参数：

- `BOT_TOKEN`
- `SUPPORT_GROUP_IDS`
- `TASK_CHANNEL_ID=-1003892387186`
- `ADMIN_TG_IDS`
- `TASK_SLA_MINUTES=10`
- PostgreSQL 参数（如需 `/task_order`）

## 运行

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a; source .env; set +a
python3 bot.py
```

## 说明

- 本项目是新仓库开发，不改动原 `hybot`。
- 任务筛选规则在 `looks_like_question()`，可按业务继续细化。
- 如果任务频道为私有 channel，请确保 bot 已加入并有发言权限。

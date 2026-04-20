import html
import logging
import os
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

UTC8 = timezone(timedelta(hours=8))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("hycs_task")


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required env: {name}")
    return str(v) if v is not None else ""


BOT_TOKEN = env("BOT_TOKEN", required=True)
SUPPORT_GROUP_IDS = {int(x.strip()) for x in env("SUPPORT_GROUP_IDS", "").split(",") if x.strip()}
TASK_CHANNEL_ID = int(env("TASK_CHANNEL_ID", required=True))
ADMIN_TG_IDS = {int(x.strip()) for x in env("ADMIN_TG_IDS", "").split(",") if x.strip()}
TASK_SLA_MINUTES = int(env("TASK_SLA_MINUTES", "10"))
QUESTION_KEYWORDS = [x.strip() for x in env("QUESTION_KEYWORDS", "").split(",") if x.strip()]
NOISE_KEYWORDS = [x.strip() for x in env("NOISE_KEYWORDS", "收到,好的,ok,thanks,谢谢,已处理").split(",") if x.strip()]
SQLITE_PATH = env("SQLITE_PATH", "tasks.db")

DB_HOST = env("DB_HOST", "")
DB_PORT = int(env("DB_PORT", "5432"))
DB_NAME = env("DB_NAME", "")
DB_USER = env("DB_USER", "")
DB_PASSWORD = env("DB_PASSWORD", "")
DB_SSLMODE = env("DB_SSLMODE", "prefer")
DB_CONNECT_TIMEOUT = int(env("DB_CONNECT_TIMEOUT", "8"))


@dataclass
class Task:
    id: int
    source_chat_id: int
    source_message_id: int
    source_user_id: int
    source_username: str
    question_text: str
    status: str
    created_at: str
    updated_at: str
    closed_at: Optional[str]
    task_channel_message_id: Optional[int]
    remind_count: int
    linked_order_no: Optional[str]
    assignee_tg_id: Optional[int]
    assignee_name: Optional[str]
    processing_at: Optional[str]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_str() -> str:
    return now_utc().isoformat(timespec="seconds")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {r[1] for r in cols}
    if col not in names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_sqlite() -> None:
    with closing(db()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                source_user_id INTEGER,
                source_username TEXT,
                question_text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                closed_at TEXT,
                task_channel_message_id INTEGER,
                remind_count INTEGER NOT NULL DEFAULT 0,
                linked_order_no TEXT,
                assignee_tg_id INTEGER,
                assignee_name TEXT,
                processing_at TEXT,
                UNIQUE(source_chat_id, source_message_id)
            );

            CREATE TABLE IF NOT EXISTS quick_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                k TEXT NOT NULL UNIQUE,
                v TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        # 兼容旧表结构
        _ensure_column(conn, "tasks", "assignee_tg_id", "assignee_tg_id INTEGER")
        _ensure_column(conn, "tasks", "assignee_name", "assignee_name TEXT")
        _ensure_column(conn, "tasks", "processing_at", "processing_at TEXT")
        conn.commit()


def is_admin(uid: Optional[int]) -> bool:
    return uid is not None and uid in ADMIN_TG_IDS


def is_support_group(chat_id: Optional[int]) -> bool:
    return chat_id is not None and chat_id in SUPPORT_GROUP_IDS


def summarize(text: str, max_len: int = 140) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    return t if len(t) <= max_len else t[: max_len - 1] + "…"


def looks_like_question(text: str) -> bool:
    s = (text or "").strip()
    if not s or len(s) < 4:
        return False
    if s.startswith("/"):
        return False

    lowered = s.lower()

    # 过滤明显噪音
    if len(s) <= 8 and any(k.lower() in lowered for k in NOISE_KEYWORDS):
        return False

    # 强规则：问号
    if "?" in s or "？" in s:
        return True

    # 强规则：订单/单号特征
    if re.search(r"(?i)(order_no|mch_order_no|订单号|单号|#)[\s:：-]*[a-z0-9_-]{4,}", s):
        return True

    # 关键词规则
    if any(k.lower() in lowered for k in QUESTION_KEYWORDS):
        return True

    # 常见客服问题短语
    if re.search(r"(没到账|未到账|未收到|失败|异常|卡单|退款|查一下|帮看|多久|为什么)", s):
        return True

    return False


def tg_message_link(chat_id: int, message_id: int) -> str:
    # 私有群/超级群: -100xxxxxxxxxx -> https://t.me/c/xxxxxxxxxx/msgid
    if str(chat_id).startswith("-100"):
        internal = str(chat_id)[4:]
        return f"https://t.me/c/{internal}/{message_id}"
    return f"chat:{chat_id}#{message_id}"


def insert_task(chat_id: int, msg_id: int, user_id: int, username: str, question_text: str) -> Optional[int]:
    with closing(db()) as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO tasks(
                    source_chat_id, source_message_id, source_user_id, source_username,
                    question_text, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?)
                """,
                (chat_id, msg_id, user_id, username, question_text, now_str(), now_str()),
            )
            conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            return None


def get_task(task_id: int) -> Optional[Task]:
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return Task(**dict(row)) if row else None


def list_open_tasks(limit: int = 30) -> list[sqlite3.Row]:
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status!='DONE' ORDER BY id DESC LIMIT ?",
            (max(1, min(limit, 100)),),
        ).fetchall()


def list_overdue_tasks(sla_minutes: int) -> list[sqlite3.Row]:
    threshold = now_utc() - timedelta(minutes=sla_minutes)
    with closing(db()) as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status='OPEN' AND datetime(created_at) <= datetime(?)",
            (threshold.isoformat(timespec="seconds"),),
        ).fetchall()


def set_task_channel_msg(task_id: int, channel_msg_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE tasks SET task_channel_message_id=?, updated_at=? WHERE id=?", (channel_msg_id, now_str(), task_id))
        conn.commit()


def mark_processing(task_id: int, assignee_id: int, assignee_name: str) -> bool:
    with closing(db()) as conn:
        cur = conn.execute(
            """
            UPDATE tasks
            SET status='PROCESSING', assignee_tg_id=?, assignee_name=?, processing_at=?, updated_at=?
            WHERE id=? AND status!='DONE'
            """,
            (assignee_id, assignee_name, now_str(), now_str(), task_id),
        )
        conn.commit()
        return cur.rowcount > 0


def mark_open(task_id: int) -> bool:
    with closing(db()) as conn:
        cur = conn.execute(
            """
            UPDATE tasks
            SET status='OPEN', assignee_tg_id=NULL, assignee_name=NULL, processing_at=NULL, updated_at=?
            WHERE id=? AND status!='DONE'
            """,
            (now_str(), task_id),
        )
        conn.commit()
        return cur.rowcount > 0


def mark_done(task_id: int) -> bool:
    with closing(db()) as conn:
        cur = conn.execute(
            "UPDATE tasks SET status='DONE', closed_at=?, updated_at=? WHERE id=? AND status!='DONE'",
            (now_str(), now_str(), task_id),
        )
        conn.commit()
        return cur.rowcount > 0


def set_task_binding(task_id: int, order_no: str) -> bool:
    with closing(db()) as conn:
        cur = conn.execute(
            "UPDATE tasks SET linked_order_no=?, updated_at=? WHERE id=?",
            (order_no.strip(), now_str(), task_id),
        )
        conn.commit()
        return cur.rowcount > 0


def inc_remind(task_id: int) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE tasks SET remind_count=remind_count+1, updated_at=? WHERE id=?", (now_str(), task_id))
        conn.commit()


def task_status_emoji(status: str) -> str:
    return {"OPEN": "🟡", "PROCESSING": "🔵", "DONE": "✅"}.get(status, "⚪")


def format_task_card(task: Task) -> str:
    created = datetime.fromisoformat(task.created_at).astimezone(UTC8).strftime("%Y-%m-%d %H:%M:%S")
    link = tg_message_link(task.source_chat_id, task.source_message_id)
    assignee = task.assignee_name or "-"
    bind = task.linked_order_no or "-"
    return (
        f"🧩 <b>客服任务 #{task.id}</b>\n"
        f"状态: {task_status_emoji(task.status)} <b>{task.status}</b>\n"
        f"来源: <code>{task.source_chat_id}</code> / msg <code>{task.source_message_id}</code>\n"
        f"提问人: {html.escape(task.source_username or str(task.source_user_id or '-'))}\n"
        f"处理人: {html.escape(assignee)}\n"
        f"绑定订单: <code>{html.escape(bind)}</code>\n"
        f"创建: {created} (UTC+8)\n"
        f"原消息: {html.escape(link)}\n"
        f"问题: {html.escape(summarize(task.question_text, 260))}\n\n"
        f"命令: /task_claim {task.id} | /task_open {task.id} | /task_done {task.id} | /task_bind {task.id} 订单号 | /task_order {task.id}"
    )


async def refresh_task_card(context: ContextTypes.DEFAULT_TYPE, task_id: int) -> None:
    task = get_task(task_id)
    if not task or not task.task_channel_message_id:
        return
    try:
        await context.bot.edit_message_text(
            chat_id=TASK_CHANNEL_ID,
            message_id=task.task_channel_message_id,
            text=format_task_card(task),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.warning("refresh_task_card failed task=%s: %s: %s", task_id, e.__class__.__name__, e)


def get_pg_conn():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        return None
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=DB_CONNECT_TIMEOUT,
    )


def query_order(order_token: str) -> Optional[dict]:
    conn = get_pg_conn()
    if conn is None:
        return None

    token = order_token.strip()
    candidates = [token, token[1:] if token.startswith("#") else f"#{token}"]

    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT o.order_no, o.mch_order_no, o.status, o.order_type, o.real_order_amount,
                       o.create_time, o.pay_time, o.finish_time, m.name AS mch_name
                FROM orders o
                LEFT JOIN mchs m ON m.id = o.mch_id
                WHERE o.order_no = ANY(%s) OR COALESCE(o.mch_order_no, '') = ANY(%s)
                ORDER BY o.id DESC LIMIT 1
                """,
                (candidates, candidates),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def fmt_order_row(order: dict) -> str:
    if not order:
        return "未找到订单。"
    return (
        f"order_no: {order.get('order_no')}\n"
        f"mch_order_no: {order.get('mch_order_no')}\n"
        f"mch_name: {order.get('mch_name')}\n"
        f"status: {order.get('status')}\n"
        f"order_type: {order.get('order_type')}\n"
        f"amount: {order.get('real_order_amount')}\n"
        f"create_time: {order.get('create_time')}\n"
        f"pay_time: {order.get('pay_time')}\n"
        f"finish_time: {order.get('finish_time')}"
    )


async def create_task_from_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, force: bool = False):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user:
        return
    if not is_support_group(chat.id):
        return
    if not force and not looks_like_question(text):
        return

    task_id = insert_task(chat.id, msg.message_id, user.id, user.full_name or user.username or str(user.id), text)
    if not task_id:
        return

    task = get_task(task_id)
    sent = await context.bot.send_message(
        chat_id=TASK_CHANNEL_ID,
        text=format_task_card(task),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    set_task_channel_msg(task_id, sent.message_id)


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text:
        return
    await create_task_from_message(update, context, msg.text, force=False)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("hycs_task v2 已启动。")


async def cmd_task_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await update.message.reply_text("未授权")
        return
    msg = update.effective_message
    if not msg or not msg.reply_to_message or not msg.reply_to_message.text:
        await update.message.reply_text("用法: 回复一条文本消息后执行 /task_new")
        return

    # 用被回复消息建任务
    src = msg.reply_to_message
    fake_text = src.text or src.caption or ""
    chat = update.effective_chat
    user = src.from_user
    if not chat:
        return
    if not is_support_group(chat.id):
        await update.message.reply_text("当前群不在 SUPPORT_GROUP_IDS")
        return

    task_id = insert_task(chat.id, src.message_id, user.id if user else 0, (user.full_name if user else "unknown"), fake_text)
    if not task_id:
        await update.message.reply_text("该消息已建过任务")
        return
    task = get_task(task_id)
    sent = await context.bot.send_message(chat_id=TASK_CHANNEL_ID, text=format_task_card(task), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    set_task_channel_msg(task_id, sent.message_id)
    await update.message.reply_text(f"✅ 已创建任务 #{task_id}")


async def cmd_task_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await update.message.reply_text("未授权")
        return
    rows = list_open_tasks(limit=30)
    if not rows:
        await update.message.reply_text("当前无未完成任务")
        return
    lines = ["未完成任务（OPEN/PROCESSING）："]
    for r in rows:
        lines.append(
            f"#{r['id']} [{r['status']}] assignee={r['assignee_name'] or '-'} remind={r['remind_count']} | "
            f"src={r['source_chat_id']}:{r['source_message_id']} | {summarize(r['question_text'], 52)}"
        )
    await update.message.reply_text("\n".join(lines)[:3900])


def _parse_task_id(args: list[str]) -> Optional[int]:
    if len(args) != 1 or not args[0].isdigit():
        return None
    return int(args[0])


async def cmd_task_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.message.reply_text("未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("用法: /task_claim <task_id>")
        return
    name = update.effective_user.full_name or update.effective_user.username or str(uid)
    ok = mark_processing(task_id, uid, name)
    if not ok:
        await update.message.reply_text("任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await update.message.reply_text(f"✅ 任务 #{task_id} 已由 {name} 认领（PROCESSING）")


async def cmd_task_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.message.reply_text("未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("用法: /task_open <task_id>")
        return
    ok = mark_open(task_id)
    if not ok:
        await update.message.reply_text("任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await update.message.reply_text(f"✅ 任务 #{task_id} 已回退为 OPEN")


async def cmd_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.message.reply_text("未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("用法: /task_done <task_id>")
        return
    ok = mark_done(task_id)
    if not ok:
        await update.message.reply_text("任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await update.message.reply_text(f"✅ 任务 #{task_id} 已完成")


async def cmd_task_bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.message.reply_text("未授权")
        return
    if len(context.args) != 2 or not context.args[0].isdigit():
        await update.message.reply_text("用法: /task_bind <task_id> <order_no>")
        return
    task_id = int(context.args[0])
    order_no = context.args[1].strip()
    ok = set_task_binding(task_id, order_no)
    if not ok:
        await update.message.reply_text("任务不存在")
        return
    await refresh_task_card(context, task_id)
    await update.message.reply_text(f"✅ 任务 #{task_id} 已绑定订单 {order_no}")


async def cmd_task_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.message.reply_text("未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("用法: /task_order <task_id>")
        return
    task = get_task(task_id)
    if not task:
        await update.message.reply_text("任务不存在")
        return
    if not task.linked_order_no:
        await update.message.reply_text("该任务未绑定订单")
        return

    order = query_order(task.linked_order_no)
    if order is None and all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        await update.message.reply_text("订单未找到")
        return
    if order is None:
        await update.message.reply_text("未配置数据库连接，无法查单")
        return
    await update.message.reply_text(fmt_order_row(order))


async def cmd_qr_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await update.message.reply_text("未授权")
        return
    if len(context.args) < 2:
        await update.message.reply_text("用法: /qr_add <key> <text>")
        return
    k = context.args[0].strip().lower()
    v = " ".join(context.args[1:]).strip()
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO quick_replies(k, v, updated_at) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
            (k, v, now_str()),
        )
        conn.commit()
    await update.message.reply_text(f"✅ 快捷语已保存: {k}")


async def cmd_qr_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with closing(db()) as conn:
        rows = conn.execute("SELECT k FROM quick_replies ORDER BY k").fetchall()
    if not rows:
        await update.message.reply_text("快捷语为空")
        return
    await update.message.reply_text("快捷语键：\n" + "\n".join([r["k"] for r in rows]))


async def cmd_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await update.message.reply_text("用法: /qr <key>")
        return
    k = context.args[0].strip().lower()
    with closing(db()) as conn:
        row = conn.execute("SELECT v FROM quick_replies WHERE k=?", (k,)).fetchone()
    if not row:
        await update.message.reply_text("未找到该快捷语")
        return
    await update.message.reply_text(row["v"])


async def job_overdue(context: ContextTypes.DEFAULT_TYPE):
    rows = list_overdue_tasks(TASK_SLA_MINUTES)
    for r in rows:
        remind_count = int(r["remind_count"] or 0)
        if remind_count >= 3:
            continue
        created = datetime.fromisoformat(r["created_at"])
        mins = int((now_utc() - created).total_seconds() // 60)
        await context.bot.send_message(
            chat_id=TASK_CHANNEL_ID,
            text=f"⏰ 任务 #{r['id']} 仍为 OPEN，已超时 {mins} 分钟（SLA={TASK_SLA_MINUTES}m）。",
        )
        inc_remind(int(r["id"]))


async def post_init(app: Application):
    await app.bot.set_my_commands(
        [
            BotCommand("start", "启动信息"),
            BotCommand("task_new", "手动建任务（回复消息）"),
            BotCommand("task_list", "列出未完成任务"),
            BotCommand("task_claim", "认领任务: /task_claim <id>"),
            BotCommand("task_open", "回退OPEN: /task_open <id>"),
            BotCommand("task_done", "完成任务: /task_done <id>"),
            BotCommand("task_bind", "绑定订单: /task_bind <id> <order_no>"),
            BotCommand("task_order", "查绑定订单: /task_order <id>"),
            BotCommand("qr_add", "新增快捷语: /qr_add <key> <text>"),
            BotCommand("qr_list", "快捷语列表"),
            BotCommand("qr", "发送快捷语: /qr <key>"),
        ]
    )


def main():
    init_sqlite()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("task_new", cmd_task_new))
    app.add_handler(CommandHandler("task_list", cmd_task_list))
    app.add_handler(CommandHandler("task_claim", cmd_task_claim))
    app.add_handler(CommandHandler("task_open", cmd_task_open))
    app.add_handler(CommandHandler("task_done", cmd_task_done))
    app.add_handler(CommandHandler("task_bind", cmd_task_bind))
    app.add_handler(CommandHandler("task_order", cmd_task_order))
    app.add_handler(CommandHandler("qr_add", cmd_qr_add))
    app.add_handler(CommandHandler("qr_list", cmd_qr_list))
    app.add_handler(CommandHandler("qr", cmd_qr))

    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_message), group=1)

    app.job_queue.run_repeating(job_overdue, interval=60, first=20, name="overdue-reminder")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

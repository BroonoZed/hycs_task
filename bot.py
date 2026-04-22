import html
import json
import logging
import os
import re
import sqlite3
import urllib.error
import urllib.request
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

UTC8 = timezone(timedelta(hours=8))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
# 避免 httpx 在 INFO 级别打印完整 Telegram URL（含 bot token）
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("hycs_task")


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required env: {name}")
    return str(v) if v is not None else ""


def set_env_key(key: str, value: str) -> None:
    env_path = Path(__file__).with_name('.env')
    lines = env_path.read_text(encoding='utf-8').splitlines() if env_path.exists() else []
    out = []
    updated = False
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            updated = True
        else:
            out.append(line)
    if not updated:
        out.append(f"{key}={value}")
    env_path.write_text("\n".join(out) + "\n", encoding='utf-8')


# 允许直接从项目目录下 .env 读取配置
if load_dotenv:
    load_dotenv(Path(__file__).with_name(".env"), override=False)
    load_dotenv(override=False)

BOT_TOKEN = env("BOT_TOKEN", required=True)
SUPPORT_GROUP_IDS = {int(x.strip()) for x in env("SUPPORT_GROUP_IDS", "").split(",") if x.strip()}
TASK_CHANNEL_ID = int(env("TASK_CHANNEL_ID", required=True))
ADMIN_TG_IDS = {int(x.strip()) for x in env("ADMIN_TG_IDS", "").split(",") if x.strip()}
TASK_SLA_MINUTES = int(env("TASK_SLA_MINUTES", "10"))
CURRENT_TASK_SLA_MINUTES = TASK_SLA_MINUTES
QUESTION_KEYWORDS = [x.strip() for x in env("QUESTION_KEYWORDS", "").split(",") if x.strip()]
NOISE_KEYWORDS = [x.strip() for x in env("NOISE_KEYWORDS", "收到,好的,ok,thanks,谢谢,已处理").split(",") if x.strip()]
NOISE_IGNORE_USER_IDS = {int(x.strip()) for x in env("NOISE_IGNORE_USER_IDS", "").split(",") if x.strip()}
# 任务快捷回复规则："触发词:快捷语key|触发词2:key2"
TASK_QR_RULES_RAW = env("TASK_QR_RULES", "")
SQLITE_PATH = env("SQLITE_PATH", "tasks.db")

ORDER_API_URL = env("ORDER_API_URL", "")
ORDER_API_TOKEN = env("ORDER_API_TOKEN", "")
ORDER_API_TIMEOUT = int(env("ORDER_API_TIMEOUT", "8"))

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
    ok = uid is not None and uid in ADMIN_TG_IDS
    if uid is not None and not ok:
        logger.info("admin_check_failed uid=%s ADMIN_TG_IDS=%s", uid, sorted(list(ADMIN_TG_IDS)))
    return ok


def is_admin_actor(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else None
    if is_admin(uid):
        return True

    # 频道内“以频道身份发送”时可能拿不到 effective_user；允许任务频道自身身份执行管理命令
    msg = update.effective_message
    chat = update.effective_chat
    sender_chat_id = msg.sender_chat.id if (msg and msg.sender_chat) else None
    if chat and chat.id == TASK_CHANNEL_ID and sender_chat_id == TASK_CHANNEL_ID:
        return True
    return False


def is_support_group(chat_id: Optional[int]) -> bool:
    return chat_id is not None and chat_id in SUPPORT_GROUP_IDS


def is_noise_ignored_user(uid: Optional[int]) -> bool:
    return uid is not None and uid in NOISE_IGNORE_USER_IDS


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


async def reply_text(update: Update, text: str, **kwargs):
    msg = update.effective_message
    if msg:
        return await msg.reply_text(text, **kwargs)
    logger.warning("No effective_message to reply. chat=%s user=%s text=%s", update.effective_chat.id if update.effective_chat else None, update.effective_user.id if update.effective_user else None, text)
    return None


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


def get_task_by_channel_msg_id(channel_msg_id: int) -> Optional[Task]:
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM tasks WHERE task_channel_message_id = ?", (channel_msg_id,)).fetchone()
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


def parse_task_qr_rules() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    raw = (TASK_QR_RULES_RAW or "").strip()
    if not raw:
        return out
    for part in raw.split("|"):
        p = part.strip()
        if not p or ":" not in p:
            continue
        trigger, key = p.split(":", 1)
        trigger = trigger.strip()
        key = key.strip().lower()
        if trigger and key:
            out.append((trigger, key))
    return out


def matched_qr_keys_for_task(task: Task, max_keys: int = 3) -> list[str]:
    txt = (task.question_text or "").lower()
    keys: list[str] = []
    for trigger, key in parse_task_qr_rules():
        if trigger.lower() in txt and key not in keys:
            keys.append(key)
            if len(keys) >= max_keys:
                break
    return keys


def task_action_kb(task: Task) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🔵 认领", callback_data=f"task:claim:{task.id}"),
            InlineKeyboardButton("🟡 回退", callback_data=f"task:open:{task.id}"),
            InlineKeyboardButton("✅ 完成", callback_data=f"task:done:{task.id}"),
        ]
    ]
    qr_keys = matched_qr_keys_for_task(task)
    if qr_keys:
        rows.append([InlineKeyboardButton(f"💬 {k}", callback_data=f"taskqr:{k}:{task.id}") for k in qr_keys])
    return InlineKeyboardMarkup(rows)


def panel_text() -> str:
    return (
        "🧰 <b>任务管理面板</b>\n"
        f"当前 SLA: <b>{CURRENT_TASK_SLA_MINUTES}</b> 分钟\n"
        "操作说明：\n"
        "- 刷新：查看未完成任务概览\n"
        "- SLA ±：快速调测试超时\n"
        "- 快速设置：一键改为常用值"
    )


def panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 刷新任务", callback_data="panel:refresh"),
            ],
            [
                InlineKeyboardButton("➖ SLA", callback_data="panel:sla:-1"),
                InlineKeyboardButton("➕ SLA", callback_data="panel:sla:+1"),
            ],
            [
                InlineKeyboardButton("SLA=1m", callback_data="panel:sla:set:1"),
                InlineKeyboardButton("SLA=3m", callback_data="panel:sla:set:3"),
                InlineKeyboardButton("SLA=10m", callback_data="panel:sla:set:10"),
            ],
        ]
    )


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
            reply_markup=task_action_kb(task),
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


def query_order_via_api(order_token: str) -> Optional[dict]:
    if not ORDER_API_URL:
        return None

    payload = json.dumps({"order_token": order_token.strip()}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if ORDER_API_TOKEN:
        headers["Authorization"] = f"Bearer {ORDER_API_TOKEN}"

    req = urllib.request.Request(ORDER_API_URL, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=ORDER_API_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        logger.warning("query_order_via_api http_error=%s", e)
        return None
    except Exception as e:
        logger.warning("query_order_via_api failed: %s: %s", e.__class__.__name__, e)
        return None

    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        return data["data"]
    if isinstance(data, dict) and data.get("ok") is False:
        return None
    return data if isinstance(data, dict) and data else None


def query_order(order_token: str) -> Optional[dict]:
    api_result = query_order_via_api(order_token)
    if api_result is not None:
        return api_result

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
    if is_noise_ignored_user(user.id):
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
        reply_markup=task_action_kb(task),
    )
    set_task_channel_msg(task_id, sent.message_id)


def extract_task_id_from_text(text: str) -> Optional[int]:
    s = (text or "").strip()
    m = re.search(r"(?:^|\s)#(\d+)(?:\s|$)", s)
    if m:
        return int(m.group(1))
    m = re.search(r"(?:task|任务)\s*#?(\d+)", s, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


async def relay_from_task_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or chat.id != TASK_CHANNEL_ID:
        return
    if user and bool(getattr(user, "is_bot", False)):
        return
    if not is_admin_actor(update):
        return

    task = None
    if msg.reply_to_message:
        task = get_task_by_channel_msg_id(msg.reply_to_message.message_id)

    if task is None and msg.text:
        tid = extract_task_id_from_text(msg.text)
        if tid:
            task = get_task(tid)

    if task is None:
        return

    # 不允许回传到已完成任务，避免误发
    if task.status == "DONE":
        await msg.reply_text(f"⚠️ 任务 #{task.id} 已是 DONE，未回传。")
        return

    try:
        sent = await context.bot.copy_message(
            chat_id=task.source_chat_id,
            from_chat_id=TASK_CHANNEL_ID,
            message_id=msg.message_id,
            reply_to_message_id=task.source_message_id,
            allow_sending_without_reply=True,
        )
        if task.status == "OPEN":
            name = user.full_name or user.username or str(user.id)
            mark_processing(task.id, user.id if user else 0, name)
            await refresh_task_card(context, task.id)
        out_msg_id = sent.message_id if hasattr(sent, "message_id") else sent
        await msg.reply_text(
            f"✅ 已回传到源群 task #{task.id} -> {task.source_chat_id}:{out_msg_id}",
            reply_to_message_id=msg.message_id,
        )
    except Exception as e:
        logger.exception("relay_from_task_channel failed task=%s", task.id)
        await msg.reply_text(f"❌ 回传失败 task #{task.id}: {e.__class__.__name__}: {e}")


def _split_cmd(text: str) -> tuple[str, list[str]]:
    t = (text or "").strip()
    if not t.startswith("/"):
        return "", []
    parts = t.split()
    cmd = parts[0][1:].split("@", 1)[0].lower()
    return cmd, parts[1:]


async def on_task_channel_command_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or chat.id != TASK_CHANNEL_ID or not msg.text:
        return

    cmd, args = _split_cmd(msg.text)
    if not cmd:
        return

    logger.info("task_channel_fallback cmd=%s args=%s chat=%s user=%s", cmd, args, chat.id, user.id if user else None)

    uid = user.id if user else None
    if not is_admin_actor(update):
        await msg.reply_text("未授权")
        return

    if cmd == "task_list":
        await msg.reply_text(build_open_task_summary(30))
        return

    if cmd == "task_panel":
        await msg.reply_text(panel_text(), parse_mode=ParseMode.HTML, reply_markup=panel_kb())
        return

    if cmd == "noise_ignore_list":
        if not NOISE_IGNORE_USER_IDS:
            await msg.reply_text("当前过滤用户名单为空")
            return
        await msg.reply_text("过滤用户ID名单：\n" + "\n".join(str(x) for x in sorted(NOISE_IGNORE_USER_IDS)))
        return

    if cmd in {"noise_ignore_add", "noise_ignore_del"}:
        if len(args) != 1 or not args[0].lstrip('-').isdigit():
            await msg.reply_text(f"用法: /{cmd} <user_id>")
            return
        uid2 = int(args[0])
        if cmd == "noise_ignore_add":
            NOISE_IGNORE_USER_IDS.add(uid2)
            set_env_key("NOISE_IGNORE_USER_IDS", ",".join(str(x) for x in sorted(NOISE_IGNORE_USER_IDS)))
            await msg.reply_text(f"✅ 已加入过滤名单: {uid2}")
        else:
            NOISE_IGNORE_USER_IDS.discard(uid2)
            set_env_key("NOISE_IGNORE_USER_IDS", ",".join(str(x) for x in sorted(NOISE_IGNORE_USER_IDS)))
            await msg.reply_text(f"✅ 已移出过滤名单: {uid2}")
        return

    if cmd in {"task_claim", "task_open", "task_done", "task_order", "task_sla"}:
        if len(args) != 1 or not args[0].isdigit():
            usage = {
                "task_claim": "/task_claim <task_id>",
                "task_open": "/task_open <task_id>",
                "task_done": "/task_done <task_id>",
                "task_order": "/task_order <task_id>",
                "task_sla": "/task_sla <minutes>",
            }[cmd]
            await msg.reply_text(f"用法: {usage}")
            return
        n = int(args[0])
        if cmd == "task_claim":
            name = (user.full_name or user.username) if user else "channel_admin"
            if not mark_processing(n, uid or 0, name):
                await msg.reply_text("任务不存在或已完成")
                return
            await refresh_task_card(context, n)
            await msg.reply_text(f"✅ 任务 #{n} 已由 {name} 认领（PROCESSING）")
            return
        if cmd == "task_open":
            if not mark_open(n):
                await msg.reply_text("任务不存在或已完成")
                return
            await refresh_task_card(context, n)
            await msg.reply_text(f"✅ 任务 #{n} 已回退为 OPEN")
            return
        if cmd == "task_done":
            if not mark_done(n):
                await msg.reply_text("任务不存在或已完成")
                return
            await refresh_task_card(context, n)
            await msg.reply_text(f"✅ 任务 #{n} 已完成")
            return
        if cmd == "task_order":
            task = get_task(n)
            if not task:
                await msg.reply_text("任务不存在")
                return
            if not task.linked_order_no:
                await msg.reply_text("该任务未绑定订单")
                return
            order = query_order(task.linked_order_no)
            if order is None and (ORDER_API_URL or all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD])):
                await msg.reply_text("订单未找到")
                return
            if order is None:
                await msg.reply_text("未配置查单接口/数据库，无法查单")
                return
            await msg.reply_text(fmt_order_row(order))
            return
        if cmd == "task_sla":
            global CURRENT_TASK_SLA_MINUTES
            if n < 1 or n > 1440:
                await msg.reply_text("SLA 范围需在 1~1440 分钟")
                return
            CURRENT_TASK_SLA_MINUTES = n
            await msg.reply_text(f"✅ 已设置超时 SLA = {CURRENT_TASK_SLA_MINUTES} 分钟（仅本次进程有效）")
            return

    if cmd == "task_bind":
        if len(args) != 2 or not args[0].isdigit():
            await msg.reply_text("用法: /task_bind <task_id> <order_no>")
            return
        task_id = int(args[0])
        order_no = args[1].strip()
        if not set_task_binding(task_id, order_no):
            await msg.reply_text("任务不存在")
            return
        await refresh_task_card(context, task_id)
        await msg.reply_text(f"✅ 任务 #{task_id} 已绑定订单 {order_no}")
        return


async def on_task_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data:
        return
    await q.answer()

    m = re.fullmatch(r"task:(claim|open|done):(\d+)", q.data)
    if not m:
        return

    action, sid = m.group(1), m.group(2)
    task_id = int(sid)
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await q.answer("未授权", show_alert=True)
        return

    if action == "claim":
        name = update.effective_user.full_name or update.effective_user.username or str(uid)
        ok = mark_processing(task_id, uid or 0, name)
        if not ok:
            await q.answer("任务不存在或已完成", show_alert=True)
            return
        await refresh_task_card(context, task_id)
        await q.answer(f"已认领 #{task_id}")
        return

    if action == "open":
        ok = mark_open(task_id)
        if not ok:
            await q.answer("任务不存在或已完成", show_alert=True)
            return
        await refresh_task_card(context, task_id)
        await q.answer(f"已回退 OPEN #{task_id}")
        return

    if action == "done":
        ok = mark_done(task_id)
        if not ok:
            await q.answer("任务不存在或已完成", show_alert=True)
            return
        await refresh_task_card(context, task_id)
        await q.answer(f"已完成 #{task_id}")
        return


async def on_task_qr_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.data:
        return
    m = re.fullmatch(r"taskqr:([a-zA-Z0-9_\-]+):(\d+)", q.data)
    if not m:
        return
    key, sid = m.group(1).lower(), m.group(2)
    task_id = int(sid)

    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await q.answer("未授权", show_alert=True)
        return

    task = get_task(task_id)
    if not task:
        await q.answer("任务不存在", show_alert=True)
        return

    with closing(db()) as conn:
        row = conn.execute("SELECT v FROM quick_replies WHERE k=?", (key,)).fetchone()
    if not row:
        await q.answer(f"未找到快捷语: {key}", show_alert=True)
        return

    try:
        await context.bot.send_message(
            chat_id=task.source_chat_id,
            text=row["v"],
            reply_to_message_id=task.source_message_id,
            allow_sending_without_reply=True,
        )
        if task.status == "OPEN":
            name = update.effective_user.full_name or update.effective_user.username or str(uid)
            mark_processing(task.id, uid or 0, name)
            await refresh_task_card(context, task.id)
        await q.answer(f"已发送: {key}")
    except Exception as e:
        logger.exception("on_task_qr_callback failed task=%s key=%s", task_id, key)
        await q.answer(f"发送失败: {e.__class__.__name__}", show_alert=True)


def build_open_task_summary(limit: int = 20) -> str:
    rows = list_open_tasks(limit=limit)
    if not rows:
        return "当前无未完成任务"
    lines = ["未完成任务（OPEN/PROCESSING）："]
    for r in rows:
        lines.append(
            f"#{r['id']} [{r['status']}] assignee={r['assignee_name'] or '-'} remind={r['remind_count']} | "
            f"{summarize(r['question_text'], 44)}"
        )
    return "\n".join(lines)[:3900]


async def cmd_task_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_actor(update):
        await reply_text(update, "未授权")
        return
    await reply_text(update, panel_text(), parse_mode=ParseMode.HTML, reply_markup=panel_kb())


async def on_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_TASK_SLA_MINUTES
    q = update.callback_query
    if not q or not q.data:
        return

    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await q.answer("未授权", show_alert=True)
        return

    data = q.data
    if data == "panel:refresh":
        await q.answer("已刷新")
        await q.message.reply_text(build_open_task_summary(20))
        try:
            await q.edit_message_text(panel_text(), parse_mode=ParseMode.HTML, reply_markup=panel_kb())
        except Exception:
            pass
        return

    m_delta = re.fullmatch(r"panel:sla:([+-])1", data)
    if m_delta:
        if m_delta.group(1) == "+":
            CURRENT_TASK_SLA_MINUTES = min(1440, CURRENT_TASK_SLA_MINUTES + 1)
        else:
            CURRENT_TASK_SLA_MINUTES = max(1, CURRENT_TASK_SLA_MINUTES - 1)
        await q.answer(f"SLA={CURRENT_TASK_SLA_MINUTES}m")
        await q.edit_message_text(panel_text(), parse_mode=ParseMode.HTML, reply_markup=panel_kb())
        return

    m_set = re.fullmatch(r"panel:sla:set:(\d+)", data)
    if m_set:
        v = int(m_set.group(1))
        if not (1 <= v <= 1440):
            await q.answer("SLA 超范围", show_alert=True)
            return
        CURRENT_TASK_SLA_MINUTES = v
        await q.answer(f"SLA={CURRENT_TASK_SLA_MINUTES}m")
        await q.edit_message_text(panel_text(), parse_mode=ParseMode.HTML, reply_markup=panel_kb())
        return


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text:
        return
    await create_task_from_message(update, context, msg.text, force=False)


async def on_command_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    text = (msg.text or "") if msg else ""
    logger.info(
        "command_seen chat_id=%s chat_type=%s user_id=%s user=%s text=%s",
        chat.id if chat else None,
        chat.type if chat else None,
        user.id if user else None,
        user.username if user else None,
        text,
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("cmd_start chat=%s user=%s", update.effective_chat.id if update.effective_chat else None, update.effective_user.id if update.effective_user else None)
    await reply_text(update, "hycs_task v2 已启动。")


async def cmd_task_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await reply_text(update, "未授权")
        return
    msg = update.effective_message
    if not msg or not msg.reply_to_message or not msg.reply_to_message.text:
        await reply_text(update, "用法: 回复一条文本消息后执行 /task_new")
        return

    # 用被回复消息建任务
    src = msg.reply_to_message
    fake_text = src.text or src.caption or ""
    chat = update.effective_chat
    user = src.from_user
    if not chat:
        return
    if not is_support_group(chat.id):
        await reply_text(update, "当前群不在 SUPPORT_GROUP_IDS")
        return

    task_id = insert_task(chat.id, src.message_id, user.id if user else 0, (user.full_name if user else "unknown"), fake_text)
    if not task_id:
        await reply_text(update, "该消息已建过任务")
        return
    task = get_task(task_id)
    sent = await context.bot.send_message(
        chat_id=TASK_CHANNEL_ID,
        text=format_task_card(task),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=task_action_kb(task),
    )
    set_task_channel_msg(task_id, sent.message_id)
    await reply_text(update, f"✅ 已创建任务 #{task_id}")


async def cmd_task_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await reply_text(update, "未授权")
        return
    rows = list_open_tasks(limit=30)
    if not rows:
        await reply_text(update, "当前无未完成任务")
        return
    lines = ["未完成任务（OPEN/PROCESSING）："]
    for r in rows:
        lines.append(
            f"#{r['id']} [{r['status']}] assignee={r['assignee_name'] or '-'} remind={r['remind_count']} | "
            f"src={r['source_chat_id']}:{r['source_message_id']} | {summarize(r['question_text'], 52)}"
        )
    await reply_text(update, "\n".join(lines)[:3900])


def _parse_task_id(args: list[str]) -> Optional[int]:
    if len(args) != 1 or not args[0].isdigit():
        return None
    return int(args[0])


async def cmd_task_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await reply_text(update, "用法: /task_claim <task_id>")
        return
    name = update.effective_user.full_name or update.effective_user.username or str(uid)
    ok = mark_processing(task_id, uid, name)
    if not ok:
        await reply_text(update, "任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await reply_text(update, f"✅ 任务 #{task_id} 已由 {name} 认领（PROCESSING）")


async def cmd_task_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await reply_text(update, "用法: /task_open <task_id>")
        return
    ok = mark_open(task_id)
    if not ok:
        await reply_text(update, "任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await reply_text(update, f"✅ 任务 #{task_id} 已回退为 OPEN")


async def cmd_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await reply_text(update, "用法: /task_done <task_id>")
        return
    ok = mark_done(task_id)
    if not ok:
        await reply_text(update, "任务不存在或已完成")
        return
    await refresh_task_card(context, task_id)
    await reply_text(update, f"✅ 任务 #{task_id} 已完成")


async def cmd_task_bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    if len(context.args) != 2 or not context.args[0].isdigit():
        await reply_text(update, "用法: /task_bind <task_id> <order_no>")
        return
    task_id = int(context.args[0])
    order_no = context.args[1].strip()
    ok = set_task_binding(task_id, order_no)
    if not ok:
        await reply_text(update, "任务不存在")
        return
    await refresh_task_card(context, task_id)
    await reply_text(update, f"✅ 任务 #{task_id} 已绑定订单 {order_no}")


async def cmd_task_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    task_id = _parse_task_id(context.args)
    if task_id is None:
        await reply_text(update, "用法: /task_order <task_id>")
        return
    task = get_task(task_id)
    if not task:
        await reply_text(update, "任务不存在")
        return
    if not task.linked_order_no:
        await reply_text(update, "该任务未绑定订单")
        return

    order = query_order(task.linked_order_no)
    if order is None and (ORDER_API_URL or all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD])):
        await reply_text(update, "订单未找到")
        return
    if order is None:
        await reply_text(update, "未配置查单接口/数据库，无法查单")
        return
    await reply_text(update, fmt_order_row(order))


async def cmd_task_sla(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_TASK_SLA_MINUTES
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await reply_text(update, "未授权")
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await reply_text(update, f"当前 SLA={CURRENT_TASK_SLA_MINUTES} 分钟。用法: /task_sla <minutes>")
        return
    minutes = int(context.args[0])
    if minutes < 1 or minutes > 1440:
        await reply_text(update, "SLA 范围需在 1~1440 分钟")
        return
    CURRENT_TASK_SLA_MINUTES = minutes
    await reply_text(update, f"✅ 已设置超时 SLA = {CURRENT_TASK_SLA_MINUTES} 分钟（仅本次进程有效）")


async def cmd_noise_ignore_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_actor(update):
        await reply_text(update, "未授权")
        return
    if not NOISE_IGNORE_USER_IDS:
        await reply_text(update, "当前过滤用户名单为空")
        return
    ids = sorted(NOISE_IGNORE_USER_IDS)
    await reply_text(update, "过滤用户ID名单：\n" + "\n".join(str(x) for x in ids))


async def cmd_noise_ignore_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_actor(update):
        await reply_text(update, "未授权")
        return
    if len(context.args) != 1 or not context.args[0].lstrip('-').isdigit():
        await reply_text(update, "用法: /noise_ignore_add <user_id>")
        return
    uid = int(context.args[0])
    NOISE_IGNORE_USER_IDS.add(uid)
    set_env_key("NOISE_IGNORE_USER_IDS", ",".join(str(x) for x in sorted(NOISE_IGNORE_USER_IDS)))
    await reply_text(update, f"✅ 已加入过滤名单: {uid}")


async def cmd_noise_ignore_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_actor(update):
        await reply_text(update, "未授权")
        return
    if len(context.args) != 1 or not context.args[0].lstrip('-').isdigit():
        await reply_text(update, "用法: /noise_ignore_del <user_id>")
        return
    uid = int(context.args[0])
    NOISE_IGNORE_USER_IDS.discard(uid)
    set_env_key("NOISE_IGNORE_USER_IDS", ",".join(str(x) for x in sorted(NOISE_IGNORE_USER_IDS)))
    await reply_text(update, f"✅ 已移出过滤名单: {uid}")


async def cmd_qr_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id if update.effective_user else None):
        await reply_text(update, "未授权")
        return
    if len(context.args) < 2:
        await reply_text(update, "用法: /qr_add <key> <text>")
        return
    k = context.args[0].strip().lower()
    v = " ".join(context.args[1:]).strip()
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO quick_replies(k, v, updated_at) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
            (k, v, now_str()),
        )
        conn.commit()
    await reply_text(update, f"✅ 快捷语已保存: {k}")


async def cmd_qr_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with closing(db()) as conn:
        rows = conn.execute("SELECT k FROM quick_replies ORDER BY k").fetchall()
    if not rows:
        await reply_text(update, "快捷语为空")
        return
    await reply_text(update, "快捷语键：\n" + "\n".join([r["k"] for r in rows]))


async def cmd_qr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) != 1:
        await reply_text(update, "用法: /qr <key>")
        return
    k = context.args[0].strip().lower()
    with closing(db()) as conn:
        row = conn.execute("SELECT v FROM quick_replies WHERE k=?", (k,)).fetchone()
    if not row:
        await reply_text(update, "未找到该快捷语")
        return
    await reply_text(update, row["v"])


async def job_overdue(context: ContextTypes.DEFAULT_TYPE):
    rows = list_overdue_tasks(CURRENT_TASK_SLA_MINUTES)
    for r in rows:
        remind_count = int(r["remind_count"] or 0)
        if remind_count >= 3:
            continue
        created = datetime.fromisoformat(r["created_at"])
        mins = int((now_utc() - created).total_seconds() // 60)
        await context.bot.send_message(
            chat_id=TASK_CHANNEL_ID,
            text=f"⏰ 任务 #{r['id']} 仍为 OPEN，已超时 {mins} 分钟（SLA={CURRENT_TASK_SLA_MINUTES}m）。",
        )
        inc_remind(int(r["id"]))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception. update=%s", update, exc_info=context.error)


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
            BotCommand("task_sla", "设置超时分钟: /task_sla <minutes>"),
            BotCommand("task_panel", "任务管理面板"),
            BotCommand("noise_ignore_list", "查看过滤用户名单"),
            BotCommand("noise_ignore_add", "加入过滤用户: /noise_ignore_add <uid>"),
            BotCommand("noise_ignore_del", "移出过滤用户: /noise_ignore_del <uid>"),
            BotCommand("qr_add", "新增快捷语: /qr_add <key> <text>"),
            BotCommand("qr_list", "快捷语列表"),
            BotCommand("qr", "发送快捷语: /qr <key>"),
        ]
    )


def main():
    init_sqlite()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(MessageHandler(filters.COMMAND, on_command_debug), group=-1)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("task_new", cmd_task_new))
    app.add_handler(CommandHandler("task_list", cmd_task_list))
    app.add_handler(CommandHandler("task_claim", cmd_task_claim))
    app.add_handler(CommandHandler("task_open", cmd_task_open))
    app.add_handler(CommandHandler("task_done", cmd_task_done))
    app.add_handler(CommandHandler("task_bind", cmd_task_bind))
    app.add_handler(CommandHandler("task_order", cmd_task_order))
    app.add_handler(CommandHandler("task_sla", cmd_task_sla))
    app.add_handler(CommandHandler("task_panel", cmd_task_panel))
    app.add_handler(CommandHandler("noise_ignore_list", cmd_noise_ignore_list))
    app.add_handler(CommandHandler("noise_ignore_add", cmd_noise_ignore_add))
    app.add_handler(CommandHandler("noise_ignore_del", cmd_noise_ignore_del))
    app.add_handler(CommandHandler("qr_add", cmd_qr_add))
    app.add_handler(CommandHandler("qr_list", cmd_qr_list))
    app.add_handler(CommandHandler("qr", cmd_qr))

    app.add_handler(CallbackQueryHandler(on_panel_callback, pattern=r"^panel:"), group=-4)
    app.add_handler(CallbackQueryHandler(on_task_qr_callback, pattern=r"^taskqr:[a-zA-Z0-9_\-]+:\d+$"), group=-3)
    app.add_handler(CallbackQueryHandler(on_task_action_callback, pattern=r"^task:(claim|open|done):\d+$"), group=-2)
    app.add_handler(MessageHandler(filters.Chat(chat_id=TASK_CHANNEL_ID) & filters.TEXT & filters.Regex(r"^/"), on_task_channel_command_fallback), group=-2)
    app.add_handler(MessageHandler(filters.Chat(chat_id=TASK_CHANNEL_ID) & (~filters.COMMAND), relay_from_task_channel), group=0)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_message), group=1)
    app.add_error_handler(on_error)

    app.job_queue.run_repeating(job_overdue, interval=60, first=20, name="overdue-reminder")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

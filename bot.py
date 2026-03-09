import os
import io
import csv
import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =========================================================
# НАСТРОЙКИ И ЛОГИ
# =========================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

WELCOME_TEXT = "https://t.me/+-20ZfSJ5Sxs3MTcy"

if not BOT_TOKEN:
    raise RuntimeError("Не найдена переменная BOT_TOKEN")

if not ADMIN_ID_RAW:
    raise RuntimeError("Не найдена переменная ADMIN_ID")

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError:
    raise RuntimeError("ADMIN_ID должен быть числом")

if not DATABASE_URL:
    raise RuntimeError("Не найдена переменная DATABASE_URL")


# =========================================================
# БАЗА ДАННЫХ
# =========================================================

def get_connection():
    """
    Каждый раз создаём новое подключение.
    Так стабильнее для Railway.
    """
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor
    )


def init_db():
    """
    Создаём таблицу users, если её ещё нет.
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        logger.info("Таблица users готова")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Ошибка init_db: %s", e)
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def save_user(user_id: int, username: str, first_name: str):
    """
    Сохраняем пользователя.
    Если уже есть — обновляем имя и username.
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO users (user_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
        """, (user_id, username, first_name))

        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Ошибка save_user: %s", e)
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_users_count() -> int:
    """
    Сколько всего пользователей.
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS count FROM users")
        row = cur.fetchone()
        return int(row["count"])

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_all_users():
    """
    Возвращает всех пользователей.
    """
    conn = None
    cur = None

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT user_id, username, first_name, joined_at
            FROM users
            ORDER BY joined_at DESC
        """)

        return cur.fetchall()

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# =========================================================
# КОМАНДЫ БОТА
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    user = update.effective_user
    username = user.username or ""
    first_name = user.first_name or ""

    logger.info("Получена команда /start от user_id=%s", user.id)

    try:
        save_user(
            user_id=user.id,
            username=username,
            first_name=first_name
        )

        await update.message.reply_text(WELCOME_TEXT)

    except Exception as e:
        logger.exception("Ошибка в /start: %s", e)
        await update.message.reply_text("Произошла ошибка при сохранении пользователя.")


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    if update.effective_user.id != ADMIN_ID:
        return

    try:
        users_count = get_users_count()

        await update.message.reply_text(
            f"📊 Админ-панель\n\nПодписчики бота: {users_count}"
        )

    except Exception as e:
        logger.exception("Ошибка в /admin: %s", e)
        await update.message.reply_text("Ошибка в админке.")


async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    if update.effective_user.id != ADMIN_ID:
        return

    try:
        rows = get_all_users()

        text_buffer = io.StringIO()
        writer = csv.writer(text_buffer)

        writer.writerow(["user_id", "username", "first_name", "joined_at"])

        for row in rows:
            joined_at = row["joined_at"]
            if isinstance(joined_at, datetime):
                joined_at = joined_at.strftime("%Y-%m-%d %H:%M:%S")

            writer.writerow([
                row["user_id"],
                row["username"] or "",
                row["first_name"] or "",
                joined_at
            ])

        binary_buffer = io.BytesIO()
        binary_buffer.write("\ufeff".encode("utf-8"))  # Excel нормально откроет UTF-8
        binary_buffer.write(text_buffer.getvalue().encode("utf-8"))
        binary_buffer.seek(0)
        binary_buffer.name = "users.csv"

        await update.message.reply_document(
            document=binary_buffer,
            filename="users.csv",
            caption=f"Всего пользователей: {len(rows)}"
        )

    except Exception as e:
        logger.exception("Ошибка в /users: %s", e)
        await update.message.reply_text("Ошибка при выгрузке пользователей.")


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    await update.message.reply_text("Бот работает ✅")


# =========================================================
# ЗАПУСК
# =========================================================

def main():
    logger.info("Проверка базы...")
    init_db()

    logger.info("Запуск бота...")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("users", users))
    app.add_handler(CommandHandler("ping", ping))

    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

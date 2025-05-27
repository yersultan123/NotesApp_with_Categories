import os
import aiosqlite
import asyncio
import psycopg2
from fastapi import HTTPException
from database.database import get_connection

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(BASE_DIR, "users.db")

_db_conn = None
_db_lock = asyncio.Lock()

async def get_db_connection():
    global _db_conn
    if _db_conn is None:
        _db_conn = await aiosqlite.connect(DB, check_same_thread=False)
    return _db_conn

async def init_db():
    async with _db_lock:
        conn = await get_db_connection()
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE,
                telegram_id INTEGER UNIQUE,
                notifications_enabled BOOLEAN DEFAULT 1
            );
        ''')
        await conn.commit()
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER,
                ttk_number TEXT,
                description TEXT,
                month_day TEXT,    
                loco_number TEXT
            );
        ''')
        await conn.commit()

async def add_user(phone: str, telegram_id: int):
    async with _db_lock:
        conn = await get_db_connection()
        try:
            await conn.execute(
                "INSERT INTO users (phone, telegram_id) VALUES (?, ?)",
                (phone, telegram_id)
            )
            await conn.commit()
        except aiosqlite.IntegrityError:
            raise ValueError("Пользователь уже существует в базе данных.")

async def check_user_by_telegram_id(telegram_id: int):
    async with _db_lock:
        conn = await get_db_connection()
        cursor = await conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user = await cursor.fetchone()
        return user

async def check_user_by_phone(phone: str):
    async with _db_lock:
        conn = await get_db_connection()
        cursor = await conn.execute("SELECT * FROM users WHERE phone = ?", (phone,))
        user = await cursor.fetchone()
        return user

async def update_notifications_status(telegram_id: int, enabled: bool):
    async with _db_lock:
        conn = await get_db_connection()
        await conn.execute(
            "UPDATE users SET notifications_enabled = ? WHERE telegram_id = ?",
            (int(enabled), telegram_id)
        )
        await conn.commit()

async def get_notifications_status(telegram_id: int):
    async with _db_lock:
        conn = await get_db_connection()
        cursor = await conn.execute("SELECT notifications_enabled FROM users WHERE telegram_id = ?", (telegram_id,))
        result = await cursor.fetchone()
        return result[0] if result else 1

async def save_task(telegram_id: int, ttk_number: str, description: str, month_day: str, loco_number: str):
    async with _db_lock:
        conn = await get_db_connection()
        print("=== save_task called ===")
        print("DB path:", os.path.abspath(DB))
        cursor = await conn.execute("SELECT id FROM user_tasks WHERE telegram_id = ? ORDER BY id ASC", (telegram_id,))
        tasks = await cursor.fetchall()
        if len(tasks) >= 5:
            id_to_delete = tasks[0][0]
            await conn.execute("DELETE FROM user_tasks WHERE id = ?", (id_to_delete,))
        await conn.execute(
            "INSERT INTO user_tasks (telegram_id, ttk_number, description, month_day, loco_number) VALUES (?, ?, ?, ?, ?)",
            (telegram_id, ttk_number, description, month_day, loco_number)
        )
        await conn.commit()
        print("Commit done.")

async def get_user_tasks(telegram_id: int):
    async with _db_lock:
        conn = await get_db_connection()
        cursor = await conn.execute(
            "SELECT ttk_number, description FROM user_tasks WHERE telegram_id = ? ORDER BY id DESC LIMIT 5",
            (telegram_id,)
        )
        tasks = await cursor.fetchall()
        return tasks

async def check_user_active(phone: str):
    query = """
        SELECT is_active FROM auth_user 
        JOIN helpdesk_employee ON auth_user.id = helpdesk_employee.user_id
        WHERE helpdesk_employee.phone = %s
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (phone,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                return False
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")

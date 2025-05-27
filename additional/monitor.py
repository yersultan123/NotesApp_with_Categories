import asyncio
import psycopg2
import psycopg2.extras

from main.bot import bot
from database.database import get_connection, get_connection2
from database import sqlite_db, settings_db
from database.settings_db import get_domain
settings_db.init_settings_db()

def get_employee_data_by_executor(executor_id: int):
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    "SELECT user_id, phone FROM helpdesk_employee WHERE user_id = %s",
                    (executor_id,)
                )
                result = cursor.fetchone()
                if result:
                    return result["user_id"], result["phone"]
                return None, None
    except Exception as e:
        print(f"Error fetching employee data for executor_id {executor_id}: {e}")
        return None, None

async def process_monitoring():
    try:
        with get_connection() as conn17:
            with conn17.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor17:
                cursor17.execute("""
                    SELECT id, created, executor_id, description, section_id
                    FROM helpdesk_ticket
                    WHERE description LIKE '%Локомотив не на связи%'
                      AND created >= NOW() - INTERVAL '24 hours'
                      AND status != 3
                """)
                tickets = cursor17.fetchall()
    except Exception as e:
        print(f"Error fetching tickets: {e}")
        return

    notifications = []
    for ticket in tickets:
        executor_id = ticket["executor_id"]
        section_id = ticket["section_id"]

        try:
            with get_connection() as conn17:
                with conn17.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor17:
                    cursor17.execute(
                        "SELECT code FROM helpdesk_locomotivesection WHERE id = %s",
                        (section_id,)
                    )
                    section_row = cursor17.fetchone()
                    if not section_row:
                        continue
                    code = section_row["code"]
        except Exception as e:
            print(f"Error fetching section for section_id {section_id}: {e}")
            continue

        try:
            with get_connection2() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT section, dt, placement FROM locomotiveipadresses
                        WHERE section = %s
                          AND dt >= NOW() - INTERVAL '5 minutes';
                        """,
                        (code,)
                    )
                    loco_row = cursor.fetchone()
                    if not loco_row:
                        continue
                    loco_section = loco_row["section"]
                    loco_dt = loco_row["dt"]
                    placement = loco_row["placement"]
        except Exception as e:
            print(f"Error fetching locomotive for code {code}: {e}")
            continue

        notifications.append({
            "executor_id": executor_id,
            "ticket_id": ticket["id"],
            "ticket_created": ticket["created"],
            "loco_section": loco_section,
            "loco_dt": loco_dt,
            "placement": placement
        })

    for notif in notifications:
        emp_user_id, phone = get_employee_data_by_executor(notif["executor_id"])
        if not phone:
            print(f"No phone found for executor_id {notif['executor_id']}")
            continue
        if not phone.startswith("+"):
            phone = "+" + phone

        async with sqlite_db._db_lock:
            db = await sqlite_db.get_db_connection()
            cursor = await db.execute(
                "SELECT telegram_id, notifications_enabled FROM users WHERE phone = ?",
                (phone,)
            )
            user_row = await cursor.fetchone()
        if not user_row:
            print(f"No telegram user found for phone {phone}")
            continue
        telegram_id, notifications_enabled = user_row
        if not notifications_enabled:
            print(f"Notifications disabled for telegram_id {telegram_id}")
            continue

        base = get_domain()
        url = f"{base}{notif['ticket_id']}/"
        message_text = (
            f"Локомотив \"{notif['loco_section']}\" вышел на связь с \"{notif['loco_dt']}\". "
            f"({notif['placement']}). Заявка \"{notif['ticket_id']}\" \"{notif['ticket_created']}\" "
            f"по описанию \"локомотив не на связи\"\nСсылка: {url}"
        )
        try:
            await bot.send_message(chat_id=telegram_id, text=message_text)
            print(f"Sent notification to telegram_id {telegram_id}: {message_text}")
        except Exception as e:
            print(f"Error sending telegram message to {telegram_id}: {e}")

async def main():
    while True:
        await process_monitoring()
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())

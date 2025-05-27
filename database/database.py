from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME1"),
        user=os.getenv("DB_USER1"),
        password=os.getenv("DB_PASSWORD1"),
        host=os.getenv("DB_HOST1"),
        port=os.getenv("DB_PORT1")
    )


def get_connection2():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME2"),
        user=os.getenv("DB_USER2"),
        password=os.getenv("DB_PASSWORD2"),
        host=os.getenv("DB_HOST2"),
        port=os.getenv("DB_PORT2")
    )

# def get_connection():
#     return psycopg2.connect(
#         dbname="rems_helpdesk",
#         user="postgres",
#         password="0000",
#         host="localhost",
#         port="5432"
#     )
#
#
#
# def get_connection2():
#     return psycopg2.connect(
#         dbname="rems_db",
#         user="postgres",
#         password="0000",
#         host="localhost",
#         port="5433"
#     )


def check_phone_in_postgres(phone: str):
    phone = f"+{phone.lstrip('+')}"
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM helpdesk_employee WHERE phone = %s",
                    (phone,)
                )
                return cursor.fetchone()
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")


async def get_full_description(ttk_number: int, year: str):
    ttk_number_with_decimal = f"{ttk_number}"
    query = """
        SELECT id, description
        FROM helpdesk_ticket
        WHERE ttk_number = %s
          AND DATE_PART('year', created_in_ttk) = %s
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (ttk_number_with_decimal, year))
                row = cursor.fetchone()
                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Description for TTK {ttk_number} in year {year} not found."
                    )
                ticket_id, description = row
                return ticket_id, description
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")
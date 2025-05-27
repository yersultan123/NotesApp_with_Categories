import asyncio
import math
import psycopg2
import psycopg2.extras
from main.bot import bot
from database.database import get_connection, get_connection2
from database import sqlite_db, settings_db
from database.settings_db import get_domain
settings_db.init_settings_db()

EARTH_RADIUS_KM = 6371.0

def haversine(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat_rad = math.radians(lat2 - lat1)
    dlon_rad = math.radians(lon2 - lon1)

    a = math.sin(dlat_rad / 2)**2 + \
        math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon_rad / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c

def bearing(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)

    x = math.sin(dlon_rad) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - \
        math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
    azimuth = math.degrees(math.atan2(x, y))
    return (azimuth + 360) % 360

async def process_tracking():
    # Load refueling points (depots)
    with get_connection2() as conn2:
        cur = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT id_point, namepoint, latitude, longitude FROM refuelingpoint")
        depots = cur.fetchall()

    with get_connection2() as conn2:
        cur = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT section, latitude, longitude, azimuth FROM locomotiveipadresses WHERE dt >= NOW() - INTERVAL '10 minutes'")
        locos = cur.fetchall()

    for loco in locos:
        section = loco['section']
        lat_l, lon_l, azi_l = loco['latitude'], loco['longitude'], loco['azimuth']

        for depot in depots:
            lat_d, lon_d = depot['latitude'], depot['longitude']
            dist = haversine(lat_l, lon_l, lat_d, lon_d)

            if 9 <= dist <= 71 and azi_l != -1:
                bearing_to_depot = bearing(lat_l, lon_l, lat_d, lon_d)
                course_diff = min((azi_l - bearing_to_depot) % 360,
                                  (bearing_to_depot - azi_l) % 360)
                if course_diff <= 20:
                    await send_ticket_messages(section, depot, dist)

            # Case 2: <10 km and azimuth unknown
            elif dist < 10 and azi_l == -1:
                await send_location_and_tickets(section, depot, dist, lat_l, lon_l)

async def send_ticket_messages(section, depot, distance):
    tickets = fetch_tickets(section)
    if not tickets:
        return
    employees = fetch_employees(depot['id_point'])
    # Build header
    lines = [
        f"🚆 Локомотив «{section}» → депо «{depot['namepoint']}»",
        f"Расстояние: {distance:.1f} км",
        "",
        "📋 *Активные заявки за 14 дней:*"
    ]
    # Append tickets
    for t in tickets:
        lines += format_ticket(t)
    message = "\n".join(lines)
    await send_bot_messages(employees, message)

async def send_location_and_tickets(section, depot, distance, lat, lon):
    tickets = fetch_tickets(section)
    if not tickets:
        return
    employees = fetch_employees(depot['id_point'])
    # Build message
    lines = [
        f"🚆 Локомотив «{section}» → депо «{depot['namepoint']}»",
        f"Расстояние: {distance:.1f} км",
        "",
        f"[Открыть в Google Maps](https://www.google.com/maps?q={lat},{lon}&z=15)",
        "",
        "📋 *Активные заявки за 14 дней:*"
    ]
    for t in tickets:
        lines += format_ticket(t)
    message = "\n".join(lines)
    await send_bot_messages(employees, message)

async def fetch_tickets(section):
    with get_connection() as conn1:
        cur = conn1.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            """
            SELECT id, created, description
            FROM helpdesk_ticket
            WHERE section_id = %s
              AND created >= NOW() - INTERVAL '14 days'
              AND status != 3
            ORDER BY created DESC
            """, (section,)
        )
        return cur.fetchall()

async def fetch_employees(depot_id):
    with get_connection() as conn1:
        cur = conn1.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            "SELECT user_id, phone FROM helpdesk_employee WHERE depot_id = %s",
            (depot_id,)
        )
        return cur.fetchall()


def format_ticket(t):
    ticket_id = t['id']
    created_str = t['created'].strftime("%Y-%m-%d %H:%M")
    desc = t['description'] or ""
    base = get_domain()
    url = f"{base}{ticket_id}/"
    return [
        f"- *#{ticket_id}* [{created_str}] {desc}",
        f"  [Ссылка на заявку #{ticket_id}]({url})",
        ""
    ]

async def send_bot_messages(employees, message):
    for emp in employees:
        phone = emp['phone']
        if not phone:
            continue
        if not phone.startswith('+'):
            phone = '+' + phone
        async with sqlite_db._db_lock:
            db = await sqlite_db.get_db_connection()
            cur = await db.execute("SELECT telegram_id FROM users WHERE phone = ?", (phone,))
            row = await cur.fetchone()
        if not row:
            continue
        tg_id = row[0]
        try:
            await bot.send_message(chat_id=tg_id, text=message, parse_mode='Markdown')
        except Exception as e:
            print(f"Ошибка отправки {tg_id}: {e}")

async def main():
    while True:
        await process_tracking()
        await asyncio.sleep(3 * 3600)

if __name__ == '__main__':
    asyncio.run(main())

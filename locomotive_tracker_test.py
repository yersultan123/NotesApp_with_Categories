import asyncio
import math
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

# ──────────────────────────────────────────────────────────────────────────────
# 1) Загрузка .env
load_dotenv()

# 2) Константы
TEST_DIR = 'test_messages'
os.makedirs(TEST_DIR, exist_ok=True)

EARTH_RADIUS_KM = 6371.0
MESSAGE_TIMEOUT = timedelta(hours=3)

# Кэш отправленных сообщений: ключ — "section-id_point", значение — datetime последней отправки
last_sent_messages: dict[str, datetime] = {}

# ──────────────────────────────────────────────────────────────────────────────
# 3) Открываем постоянные соединения к БД
conn_helpdesk = psycopg2.connect(
    dbname=os.getenv("DB_NAME1"),
    user=os.getenv("DB_USER1"),
    password=os.getenv("DB_PASSWORD1"),
    host=os.getenv("DB_HOST1"),
    port=os.getenv("DB_PORT1")
)
conn_loco = psycopg2.connect(
    dbname=os.getenv("DB_NAME2"),
    user=os.getenv("DB_USER2"),
    password=os.getenv("DB_PASSWORD2"),
    host=os.getenv("DB_HOST2"),
    port=os.getenv("DB_PORT2")
)

# ──────────────────────────────────────────────────────────────────────────────
# 4) Вспомогательные функции гео
def haversine(lat1, lon1, lat2, lon2):
    lat1_rad, lat2_rad = map(math.radians, (lat1, lat2))
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c

def bearing(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon_rad = math.radians(lon2 - lon1)
    x = math.sin(dlon_rad) * math.cos(lat2_rad)
    y = math.cos(lat1_rad)*math.sin(lat2_rad) - \
        math.sin(lat1_rad)*math.cos(lat2_rad)*math.cos(dlon_rad)
    azimuth = math.degrees(math.atan2(x, y))
    return (azimuth + 360) % 360

# ──────────────────────────────────────────────────────────────────────────────
# 5) Основная логика опроса и отправки

async def process_tracking():
    print("[TRACE] process_tracking started")

    # 5.1. Загружаем депо и локомотивы разными курсорами на одном соединении
    cur2 = conn_loco.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur2.execute("SELECT id_point, namepoint, latitude, longitude FROM refuelingpoint")
        depots = cur2.fetchall()

        cur2.execute(
            "SELECT section, latitude, longitude, azimuth "
            "FROM locomotiveipadresses "
            "WHERE dt >= NOW() - INTERVAL '10 minutes'"
        )
        locos = cur2.fetchall()
    finally:
        cur2.close()

    print(f"[TRACE] Loaded {len(depots)} depots and {len(locos)} locomotive records")

    for loco in locos:
        section = loco['section']
        lat_l = loco['latitude']
        lon_l = loco['longitude']
        azi_l = loco['azimuth']

        if None in (lat_l, lon_l):
            print(f"[WARN] Skipping loco {section}: no coords")
            continue

        for depot in depots:
            depot_id   = depot['id_point']
            depot_name = depot['namepoint']
            lat_d      = depot['latitude']
            lon_d      = depot['longitude']
            if None in (lat_d, lon_d):
                continue

            dist = haversine(lat_l, lon_l, lat_d, lon_d)
            print(f"[TRACE] Loco {section} → Depot {depot_name}: {dist:.1f} km")

            # Проверяем кэш по ключу "section-depot_id"
            key = f"{section}-{depot_id}"
            now = datetime.utcnow()
            last = last_sent_messages.get(key)
            if last and now - last < MESSAGE_TIMEOUT:
                print(f"[SKIP] Recent msg for {key}, {now-last} ago")
                continue

            # Определяем, нужно ли слать сообщение
            msg = ''
            # Случай 1: 9–71 км и валидный азимут
            if 9 <= dist <= 71 and azi_l not in (None, -1):
                brg = bearing(lat_l, lon_l, lat_d, lon_d)
                diff = min((azi_l - brg) % 360, (brg - azi_l) % 360)
                if diff <= 20:
                    print(f"[INFO] Loco {section} approaching {depot_name}, diff={diff:.1f}°")
                    msg = build_ticket_message(section, depot_name, dist)
            # Случай 2: <10 км и азимут неизвестен
            elif dist < 10 and azi_l == -1:
                print(f"[INFO] Loco {section} within 10 km of {depot_name}, azimuth unknown")
                msg = build_location_message(section, depot_name, dist, lat_l, lon_l)

            if msg:
                write_message(depot_name, msg)
                last_sent_messages[key] = now

# ──────────────────────────────────────────────────────────────────────────────
# 6) Функции для работы с тикетами и формированием текста

def fetch_tickets(section):
    cur1 = conn_helpdesk.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur1.execute("""
            SELECT id, created, description
              FROM helpdesk_ticket
             WHERE section_id = %s
               AND created >= NOW() - INTERVAL '14 days'
               AND status != 3
             ORDER BY created DESC
        """, (section,))
        tickets = cur1.fetchall()
        print(f"[TRACE] Fetched {len(tickets)} tickets for {section}")
        return tickets
    finally:
        cur1.close()

def format_ticket(t):
    ticket_id  = t['id']
    created = t['created'].strftime("%Y-%m-%d %H:%M")
    desc = t['description'] or ""
    url = f"http://remshelpdesk.railverse.kz/helpdesk/ticket/{ticket_id}/"
    return [
        f"- *#{ticket_id}* [{created}] {desc}",
        f"  [Ссылка на заявку #{ticket_id}]({url})",
        ""
    ]

def build_ticket_message(section, depo, distance):
    tickets = fetch_tickets(section)
    if not tickets:
        return ''
    lines = [
        f"🚆 Локомотив «{section}» → депо «{depo}»",
        f"Расстояние: {distance:.1f} км",
        "",
        "📋 *Активные заявки за 14 дней:*"
    ]
    for t in tickets:
        lines.extend(format_ticket(t))
    return "\n".join(lines)

def build_location_message(section, depo, distance, lat, lon):
    tickets = fetch_tickets(section)
    if not tickets:
        return ''
    maps_url = f"https://www.google.com/maps?q={lat},{lon}&z=15"
    lines = [
        f"🚆 Локомотив «{section}» → депо «{depo}»",
        f"Расстояние: {distance:.1f} км",
        "",
        f"[Открыть в Google Maps]({maps_url})",
        "",
        "📋 *Активные заявки за 14 дней:*"
    ]
    for t in tickets:
        lines.extend(format_ticket(t))
    return "\n".join(lines)

def write_message(depo, message):
    if not message:
        return
    safe = depo.replace(' ', '_')
    path = os.path.join(TEST_DIR, f"{safe}.txt")
    print(f"[WRITE] → {path}")
    with open(path, 'a', encoding='utf-8') as f:
        f.write(message + "\n\n---\n\n")

# ──────────────────────────────────────────────────────────────────────────────
# 7) Запуск цикла
async def main():
    print("[START] Bot starting")
    try:
        while True:
            await process_tracking()
            print("[SLEEP] Sleeping for 5 mins")
            await asyncio.sleep(5 * 60)
    finally:
        # На выходе закрываем соединения
        conn_helpdesk.close()
        conn_loco.close()

if __name__ == '__main__':
    asyncio.run(main())

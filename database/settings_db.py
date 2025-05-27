import os
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB = os.path.join(BASE_DIR, "users.db")

def init_settings_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    # Таблица для хранения текущего домена
    cur.execute("""
      CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    """)
    # История изменений
    cur.execute("""
      CREATE TABLE IF NOT EXISTS domain_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        old_value TEXT,
        new_value TEXT,
        changed_by TEXT,
        changed_at TEXT
      )
    """)
    conn.commit()
    conn.close()

def get_domain():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key='domain'")
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def set_domain(new_domain: str, changer_phone: str):
    old = get_domain()
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    if old is None:
        cur.execute("INSERT INTO settings (key, value) VALUES ('domain', ?)", (new_domain,))
    else:
        cur.execute("UPDATE settings SET value = ? WHERE key='domain'", (new_domain,))
    cur.execute(
        "INSERT INTO domain_history (old_value, new_value, changed_by, changed_at) VALUES (?, ?, ?, ?)",
        (old, new_domain, changer_phone, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()
    print("изменен домен на:", new_domain)

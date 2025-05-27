import os

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, validator
from bot import bot
from database import settings_db
from database.sqlite_db import check_user_by_phone, get_notifications_status, save_task
from database.database import get_full_description
from database.settings_db import get_domain
import re

settings_db.init_settings_db()

API_KEY = os.getenv("API_KEY")
app = FastAPI()

class TaskRequest(BaseModel):
    phone: str
    body: str

    @validator("phone")
    def validate_phone(cls, phone):
        return phone

    @validator("body")
    def validate_body(cls, body):
        if not body.strip():
            raise ValueError("Текст задания не может быть пустым.")
        return body

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Недействительный API ключ.")

def extract_ttk_date_loco(message: str):
    ttk_match = re.search(r'ТТК (\d+)', message)
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', message)
    loco_match = re.search(r'\d{2}:\d{2} (.+)', message)
    if ttk_match and date_match and loco_match:
        ttk_number = int(ttk_match.group(1))
        year, month, day = date_match.groups()
        loco_number = loco_match.group(1).strip()
        month_day = f"{month}-{day}"
        return ttk_number, year, month_day, loco_number
    raise HTTPException(status_code=400, detail="Не удалось извлечь номер TTK, год, месяц, день или номер локомотива из сообщения.")


@app.post("/send-task/", dependencies=[Depends(verify_api_key)])
async def send_task(data: TaskRequest):
    print("Received data:", data.dict())

    ttk_number, year, month_day, loco_number = extract_ttk_date_loco(data.body)
    ticket_id, full_description = await get_full_description(ttk_number, year)
    user = await check_user_by_phone(data.phone)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь с указанным номером не найден.")
    telegram_id = user[2]
    notifications_enabled = await get_notifications_status(telegram_id)
    if not notifications_enabled:
        raise HTTPException(status_code=403, detail="Пользователь отключил получение заданий.")

    base = get_domain()
    if not base:
        raise HTTPException(status_code=500, detail="Домен не задан в настройках.")
    url = f"{base}{ticket_id}/"
    message_text = (
        f"📋 {full_description}\n\n"
        f"<a href=\"{url}\">Ссылка на заявку #{ticket_id}</a>"
    )
    await bot.send_message(
        chat_id=telegram_id,
        text=message_text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    await save_task(telegram_id, ttk_number, message_text, month_day, loco_number)
    return {"status": "Задание успешно отправлено."}

import os
import sqlite3

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from aiogram.filters import Command
from database.database import check_phone_in_postgres
from database.sqlite_db import check_user_by_telegram_id

API_TOKEN = os.getenv("ADMIN_TG_API_KEY")
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class AdminStates(StatesGroup):
    waiting_for_contact = State()
    waiting_for_new_domain = State()

def admin_keyboard():
    buttons = [
        [KeyboardButton(text="📡 Показать текущий домен")],
        [KeyboardButton(text="🕘 История изменений")],
        [KeyboardButton(text="✏️ Изменить домен")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


@dp.message(Command(commands=["start"]))
async def cmd_start(message: types.Message, state: FSMContext):
    settings_db.init_settings_db()
    existing = await check_user_by_telegram_id(message.from_user.id)
    if existing:
        await message.answer(
            "Добро пожаловать обратно! Вот доступные команды:",
            reply_markup=admin_keyboard()
        )
        return
    await message.answer(
        "Пожалуйста, отправьте свой номер телефона через кнопку ниже.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Отправить номер телефона", request_contact=True)]
            ],
            resize_keyboard=True
        )
    )
    await state.set_state(AdminStates.waiting_for_contact)


@dp.message(lambda msg: msg.contact is not None, AdminStates.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    phone = f"+{message.contact.phone_number.lstrip('+')}"
    user_data = check_phone_in_postgres(phone)
    if user_data:
        await message.answer("Доступ предоставлен.", reply_markup=admin_keyboard())
        await state.clear()
    else:
        await message.answer("Ваш номер не найден — доступа нет.")


@dp.message(lambda msg: msg.text == "📡 Показать текущий домен")
async def show_domain(message: types.Message):
    domain = settings_db.get_domain()
    if domain:
        await message.answer(f"Текущий домен:\n{domain}")
    else:
        await message.answer("Домен ещё не задан.")


@dp.message(lambda msg: msg.text == "🕘 История изменений")
async def history(message: types.Message):
    conn = sqlite3.connect(settings_db.DB)
    cur = conn.cursor()
    cur.execute("SELECT old_value, new_value, changed_by, changed_at FROM domain_history ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        await message.answer("Изменений ещё не было.")
        return
    text = ["История последних изменений:"]
    for old, new, by, at in rows:
        text.append(f"{at}: {old or '[пусто]'} → {new} (by {by})")
    await message.answer("\n".join(text))


@dp.message(lambda msg: msg.text == "✏️ Изменить домен")
async def edit_domain(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите новый домен в формате:\n"
    )
    await state.set_state(AdminStates.waiting_for_new_domain)


@dp.message(AdminStates.waiting_for_new_domain)
async def process_new_domain(message: types.Message, state: FSMContext):
    new = message.text.strip()
    settings_db.set_domain(new, changer_phone=str(message.from_user.id))
    await message.answer(
        f"Домен обновлён на:\n{new}\n\n"
        "Домен должен быть в таком формате:\n"
    , reply_markup=admin_keyboard())
    await state.clear()


if __name__ == "__main__":
    import asyncio

    from database import settings_db
    settings_db.init_settings_db()
    asyncio.run(dp.start_polling(bot, skip_updates=True))

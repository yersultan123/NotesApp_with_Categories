import asyncio
import io
import logging
import tempfile
from functools import wraps
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
from aiogram.types.input_file import FSInputFile, BufferedInputFile
from dotenv import load_dotenv
import os
from database.sqlite_db import (
    add_user,
    check_user_by_telegram_id,
    init_db,
    update_notifications_status,
    get_notifications_status,
    DB,
    check_user_active
)
from database.database import check_phone_in_postgres
from additional import CSVcorrector

if not os.path.exists("../downloads"):
    os.makedirs("../downloads")

load_dotenv()

API_TOKEN = os.getenv("TG_API_KEY")
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


class UserDataState(StatesGroup):
    waiting_for_name = State()


class UploadCSV(StatesGroup):
    waiting_for_file = State()


class CSVCorrection(StatesGroup):
    waiting_for_file = State()


def get_main_keyboard(is_enabled: bool):
    notification_text = "🔔 Уведомления" if is_enabled else "🔕 Уведомления"
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=notification_text), KeyboardButton(text="📋 Заявки")],
            [KeyboardButton(text="⚙️ Корректировать CSV")]
        ],
        resize_keyboard=True
    )


def check_user_active_decorator(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        telegram_id = message.from_user.id
        user = await check_user_by_telegram_id(telegram_id)
        if not user:
            await message.answer("Ваш аккаунт не найден.")
            return
        phone = user[1]
        is_active = await check_user_active(phone)
        if not is_active:
            await message.answer("Вы не активны.")
            return
        return await handler(message, *args, **kwargs)
    return wrapper


async def process_csv_file(file_path: str) -> str:
    loop = asyncio.get_running_loop()
    processed_content = await loop.run_in_executor(None, process_csv_sync, file_path)
    with tempfile.NamedTemporaryFile(delete=False, suffix='_corrected.csv', mode='w', encoding='utf-8') as tmp:
        tmp.write(processed_content)
        return tmp.name


def process_csv_sync(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return CSVcorrector.process_csv(content)


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user = await check_user_by_telegram_id(message.from_user.id)
    if user:
        notifications_enabled = await get_notifications_status(message.from_user.id)
        await message.answer("Вы уже авторизованы!", reply_markup=get_main_keyboard(notifications_enabled))
    else:
        phone_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отправить номер телефона", request_contact=True)]],
            resize_keyboard=True
        )
        await message.answer("Пожалуйста, отправьте свой номер телефона.", reply_markup=phone_keyboard)


@dp.message(lambda message: message.contact is not None)
async def process_contact(message: types.Message):
    phone = f"+{message.contact.phone_number.lstrip('+')}"
    loop = asyncio.get_running_loop()
    user_data = await loop.run_in_executor(None, lambda: check_phone_in_postgres(phone))
    if user_data:
        telegram_id = message.from_user.id
        await add_user(phone, telegram_id)
        await message.answer("Ваш номер зарегистрирован!", reply_markup=get_main_keyboard(True))
    else:
        await message.answer("Ваш номер отсутствует в базе данных.")


@dp.message(lambda message: message.text in ["🔔 Уведомления", "🔕 Уведомления"])
@check_user_active_decorator
async def toggle_notifications(message: types.Message):
    telegram_id = message.from_user.id
    current_status = await get_notifications_status(telegram_id)
    new_status = not current_status
    await update_notifications_status(telegram_id, new_status)
    status_text = "включили" if new_status else "отключили"
    await message.answer(f"Вы {status_text} отправку заявок.", reply_markup=get_main_keyboard(new_status))


@dp.message(lambda message: message.text == "📋 Заявки")
@check_user_active_decorator
async def show_tasks(message: types.Message):
    telegram_id = message.from_user.id
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "SELECT ttk_number, month_day, loco_number FROM user_tasks WHERE telegram_id = ? ORDER BY id DESC LIMIT 5",
            (telegram_id,)
        )
        tasks = await cursor.fetchall()
        if tasks:
            task_buttons = [
                [KeyboardButton(text=f"заявка: {task[0]}, {task[1]}, {task[2]}")]
                for task in tasks
            ]
            task_buttons.append([KeyboardButton(text="Назад"), KeyboardButton(text="Обновить")])
            task_keyboard = ReplyKeyboardMarkup(keyboard=task_buttons, resize_keyboard=True)
            await message.answer("Ваши заявки:", reply_markup=task_keyboard)
        else:
            await message.answer(
                "У вас нет назначенных заявок.",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="Назад"), KeyboardButton(text="Обновить")]],
                    resize_keyboard=True
                )
            )


@dp.message(lambda message: message.text == "Обновить")
@check_user_active_decorator
async def refresh_tasks(message: types.Message):
    await show_tasks(message)


@dp.message(lambda message: message.text is not None and message.text.startswith("заявка:"))
async def send_task_description(message: types.Message):
    try:
        ttk_number = message.text.split(":")[1].split(",")[0].strip()
    except IndexError:
        await message.answer("Произошла ошибка при обработке вашего запроса.")
        return

    telegram_id = message.from_user.id
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "SELECT description FROM user_tasks WHERE telegram_id = ? AND ttk_number = ?",
            (telegram_id, ttk_number)
        )
        row = await cursor.fetchone()

    if not row:
        await message.answer("Заявка не найдена или описание отсутствует.")
        return
    description_with_link = row[0]
    await message.answer(
        f"📋 Описание заявки {ttk_number}:\n\n{description_with_link}",
        parse_mode="HTML",
        disable_web_page_preview=True
    )



@dp.message(lambda message: message.text == "Назад")
@check_user_active_decorator
async def go_back(message: types.Message):
    notifications_enabled = await get_notifications_status(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=get_main_keyboard(notifications_enabled))


@dp.message(lambda message: message.text == "📂 Загрузить CSV")
async def request_file(message: types.Message, state: FSMContext):
    await message.answer("Пожалуйста, отправьте CSV-файл.")
    await state.set_state(UploadCSV.waiting_for_file)


async def upload_csv_filter(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    return current_state == UploadCSV.waiting_for_file and message.document is not None


@dp.message(upload_csv_filter)
async def handle_file(message: types.Message, state: FSMContext):
    document = message.document
    if not document.file_name.endswith('.csv'):
        await message.answer("Пожалуйста, отправьте файл в формате CSV.")
        return
    file_path = f"downloads/{document.file_name}"
    try:
        file_info = await bot.get_file(document.file_id)
        file_bytes = await bot.download_file(file_info.file_path)
        with open(file_path, 'wb') as f:
            f.write(file_bytes)
        processed_file_path = await process_csv_file(file_path)
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла: {e}")
        await message.answer("Произошла ошибка при обработке файла. Попробуйте снова.")
        await state.clear()
        return


    if processed_file_path:
        input_file = FSInputFile(processed_file_path)
        await message.answer_document(input_file, caption="Вот ваш обработанный файл!")
        try:
            os.remove(processed_file_path)
        except Exception as e:
            logging.error(f"Ошибка при удалении обработанного файла {processed_file_path}: {e}")
    else:
        await message.answer("Ошибка при обработке файла.")
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            logging.error(f"Ошибка при удалении исходного файла {file_path}: {e}")
    await state.clear()



@dp.message(lambda message: message.text == "⚙️ Корректировать CSV")
async def request_csv_correction(message: types.Message, state: FSMContext):
    await message.answer("Пожалуйста, отправьте CSV-файл для корректировки.")
    await state.set_state(CSVCorrection.waiting_for_file)


async def csv_correction_filter(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    return current_state == CSVCorrection.waiting_for_file and message.document is not None


@dp.message(csv_correction_filter)
async def handle_csv_correction(message: types.Message, state: FSMContext):
    document = message.document
    if not document.file_name.endswith('.csv'):
        await message.answer("Пожалуйста, отправьте файл в формате CSV.")
        return
    try:
        file_info = await bot.get_file(document.file_id)
        file_obj = await bot.download_file(file_info.file_path)
        if isinstance(file_obj, io.BytesIO):
            file_content = file_obj.getvalue().decode('utf-8')
        else:
            file_content = file_obj.decode('utf-8')
        processed_content = CSVcorrector.process_csv(file_content)
        processed_file = io.BytesIO(processed_content.encode('utf-8'))
        input_file = BufferedInputFile(processed_file.getvalue(), filename=document.file_name.replace('.csv', '_corrected.csv'))
        await message.answer_document(input_file, caption="⚙️ Вот ваш обработанный файл!")
    except Exception as e:
        await message.answer(f"Произошла ошибка при обработке файла: {e}")
    await state.clear()


async def main():
    await init_db()
    print("Бот запущен...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())

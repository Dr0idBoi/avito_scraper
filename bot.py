#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ЛОКАЛЬНЫЙ ТГ-БОТ (python-telegram-bot v20)
— Команда: /parse <url> <count>
— Запускает avito_scraper.scrape(...) и присылает один JSON-файл

Перед первым запуском установите зависимости:
  pip install python-telegram-bot==20.* playwright bs4 lxml

И один раз поставьте браузер:
  playwright install

Затем задайте BOT_TOKEN через переменную окружения ИЛИ вставьте прямо в код ниже.
"""

import os
import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse

from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, ContextTypes

# Импорт нашего скрапера
import avito_scraper

# ---- Токен бота ----
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip() or "7051340892:AAFAxEGGGefLS6gHLM6uMaoQ8vKMaVXmd10"

# ---- Логгирование (чтобы видеть ошибки) ----
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("avito-bot")

def _looks_like_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return bool(u.scheme and u.netloc)
    except Exception:
        return False

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришлите команду в формате:\n"
        "/parse <ссылка_на_категорию_или_поиск> <кол-во>\n\n"
        "Пример:\n"
        "/parse https://www.avito.ru/moskva/velosipedy?cd=1 3"
    )

async def parse_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # разбираем аргументы: ожидаем 2 — url и число
    args = context.args
    if len(args) < 2 or not _looks_like_url(args[0]):
        await update.message.reply_text("Формат: /parse <url> <count>\nПример: /parse https://www.avito.ru/moskva/velosipedy?cd=1 5")
        return
    url = args[0]
    try:
        count = int(args[1])
    except Exception:
        await update.message.reply_text("Второй аргумент должен быть числом. Пример: /parse <url> 5")
        return
    count = max(1, min(count, 30))  # на локалке не злоупотребляем

    msg = await update.message.reply_text("Запускаю сбор… Откроется окно браузера. Если потребуется — решите капчу.")
    try:
        out_path = await avito_scraper.scrape(url, count)  # запускаем асинхронно в том же loop
    except Exception as e:
        logger.exception("Scrape failed")
        await msg.edit_text(f"Не удалось собрать данные: {e}")
        return

    # шлём файл
    try:
        fn = Path(out_path)
        if not fn.exists():
            await msg.edit_text("Сбор завершён, но файл не найден 🤔")
            return
        await msg.edit_text("Готово! Отправляю файл…")
        with fn.open("rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=fn.name),
                caption=f"{fn.name} • {fn.stat().st_size} байт"
            )
    finally:
        # На локалке можно оставить файлы. Если хотите — раскомментируйте удаление:
        # try: fn.unlink(missing_ok=True)
        # except Exception: pass
        pass

def main():
    if not BOT_TOKEN or BOT_TOKEN.startswith("PASTE_"):
        raise SystemExit("Укажите токен бота в BOT_TOKEN или переменной окружения.")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("parse", parse_cmd))

    logger.info("Bot started. Press Ctrl+C to stop.")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram Bot (Bot #1): принимает ссылку на категорию Avito и число объявлений,
запускает серверную версию скрейпера и отправляет пользователю JSON-файл.

Зависимости:
  python-telegram-bot>=21.0
  playwright
  bs4, httpx, jsonschema, python-dotenv (если нужны)
Запуск:
  export TG_TOKEN="123:ABC"...   # или положи в .env
  python bot_server.py

Webhook-режим (опционально):
  export WEBHOOK_URL="https://your.domain/bot"
  export PORT="8080"
  python bot_server.py
"""

import os
import re
import io
import json
import time
import asyncio
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)

# локальные модули
import avito_scraper  # твой обновлённый серверный скрейпер

# -------------------- загрузка env --------------------
load_dotenv()
TOKEN = os.getenv("TG_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("TG_TOKEN не задан")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()  # если пусто — будет long polling
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/bot").strip() or "/bot"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

# По умолчанию для сервера хотим headless-браузер:
os.environ.setdefault("HEADLESS", "1")  # см. avito_scraper.py

EXPORTS_DIR = Path("./exports")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Чтобы не было гонок в одном чате — локи на чат
_chat_locks: dict[int, asyncio.Lock] = {}


# -------------------- утилиты --------------------
URL_RE = re.compile(r"https?://(?:www\.)?avito\.ru/[^\s]+", re.I)

def _parse_args_from_text(text: str) -> Tuple[Optional[str], int]:
    """
    Достаём первую avito-ссылку и число объявлений (по умолчанию 5).
    Примеры:
      /scrape https://www.avito.ru/moskva/velosipedy?cd=1 3
      https://www.avito.ru/moskva_i_mo?q=морские+контейнеры 10
    """
    url = None
    m = URL_RE.search(text or "")
    if m:
        url = m.group(0).strip()

    # число — последнее целое в тексте (необязательно)
    count = 5
    nums = re.findall(r"\b(\d{1,3})\b", text or "")
    if nums:
        try:
            count = max(1, min(50, int(nums[-1])))
        except Exception:
            pass

    return url, count


def _jsonl_to_array_bytes(jsonl_path: Path) -> bytes:
    arr = []
    if jsonl_path.exists():
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    arr.append(json.loads(line))
                except Exception:
                    # пропускаем битые строки
                    pass
    return json.dumps(arr, ensure_ascii=False, indent=2).encode("utf-8")


async def _send_typing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: int):
    try:
        await ctx.bot.send_chat_action(chat_id=chat_id, action="typing")
    except Exception:
        pass


# -------------------- хендлеры --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли ссылку на категорию Avito и количество объявлений.\n"
        "Пример:\n"
        "https://www.avito.ru/moskva/velosipedy?cd=1 3\n\n"
        "Или командой: /scrape <url> <count>"
    )


async def scrape_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = " ".join(context.args) if context.args else (update.message.text or "")
    await _handle_scrape(update, context, text)


async def any_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    await _handle_scrape(update, context, text)


async def _handle_scrape(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    chat_id = update.effective_chat.id
    url, count = _parse_args_from_text(text)

    if not url:
        await update.message.reply_text(
            "Не вижу ссылки на Avito. Пришли что-то вроде:\n"
            "https://www.avito.ru/moskva/velosipedy?cd=1 3"
        )
        return

    # единовременный запуск в чате
    lock = _chat_locks.setdefault(chat_id, asyncio.Lock())
    if lock.locked():
        await update.message.reply_text("⚠️ Уже идёт сбор по этому чату. Дождитесь завершения.")
        return

    async with lock:
        msg = await update.message.reply_text(
            f"Стартую сбор: \n• URL: {url}\n• Объявлений: {count}\n"
            f"Это может занять немного времени."
        )

        try:
            # чистим предыдущий результат JSONL (скрейпер делает это сам, но на всякий)
            out_jsonl = avito_scraper.OUTPUT_JSONL
            if isinstance(out_jsonl, str):
                out_jsonl = Path(out_jsonl)
            elif isinstance(out_jsonl, Path):
                pass
            else:
                out_jsonl = Path("./data/avito_items.jsonl")
            out_jsonl.parent.mkdir(parents=True, exist_ok=True)

            await _send_typing(context, chat_id)

            # Запускаем скрейпер
            await avito_scraper.run(url, count, skip_robots=False)

            await _send_typing(context, chat_id)

            # Формируем аккуратный JSON-массив из JSONL
            payload = _jsonl_to_array_bytes(out_jsonl)

            # Имя файла с меткой времени и chat_id
            ts = int(time.time())
            fname = f"avito_{chat_id}_{ts}.json"
            fbytes = io.BytesIO(payload)
            fbytes.name = fname

            await update.message.reply_document(
                document=InputFile(fbytes, filename=fname),
                caption="Готово ✅\nОтправляю JSON с карточками."
            )

            try:
                # Параллельно сохраним в exports (на сервере пригодится)
                (EXPORTS_DIR / fname).write_bytes(payload)
            except Exception:
                pass

        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")
        finally:
            try:
                await msg.delete()
            except Exception:
                pass


# -------------------- точка входа --------------------
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scrape", scrape_cmd))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), any_text))

    if WEBHOOK_URL:
        # webhook режим
        await app.bot.set_webhook(url=WEBHOOK_URL, drop_pending_updates=True)
        await app.run_webhook(
            listen=HOST, port=PORT, url_path=WEBHOOK_PATH,
            webhook_url=WEBHOOK_URL,  # для совместимости PTB
        )
    else:
        # long polling режим (проще для старта)
        await app.run_polling(close_loop=False, drop_pending_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass

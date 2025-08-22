#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import io
import json
import time
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

import avito_scraper  # твой серверный скрейпер

load_dotenv()
TOKEN = os.getenv("TG_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("TG_TOKEN не задан")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()  # если пусто — используем long polling
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/bot").strip() or "/bot"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

os.environ.setdefault("HEADLESS", "1")

EXPORTS_DIR = Path("./exports")
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------- utils ----------
URL_RE = re.compile(r"https?://(?:www\.)?avito\.ru/[^\s]+", re.I)

def _parse_args_from_text(text: str) -> Tuple[Optional[str], int]:
    url = None
    m = URL_RE.search(text or "")
    if m:
        url = m.group(0).strip()
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
                    pass
    return json.dumps(arr, ensure_ascii=False, indent=2).encode("utf-8")

# ---------- handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли ссылку на категорию Avito и количество объявлений.\n"
        "Пример: https://www.avito.ru/moskva/velosipedy?cd=1 3\n"
        "Или командой: /scrape <url> <count>"
    )

async def scrape_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = " ".join(context.args) if context.args else (update.message.text or "")
    await _handle_scrape(update, context, text)

async def any_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    await _handle_scrape(update, context, text)

# лок на чат, чтобы не запускать несколько сборов параллельно
_chat_locks: dict[int, "asyncio.Lock"] = {}

async def _handle_scrape(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    import asyncio  # локальный импорт, чтобы не мешал при синхронном main()
    chat_id = update.effective_chat.id
    url, count = _parse_args_from_text(text)

    if not url:
        await update.message.reply_text(
            "Не вижу ссылки на Avito. Пришли, например:\n"
            "https://www.avito.ru/moskva/velosipedy?cd=1 3"
        )
        return

    lock = _chat_locks.setdefault(chat_id, asyncio.Lock())
    if lock.locked():
        await update.message.reply_text("⚠️ Уже идёт сбор по этому чату. Дождитесь завершения.")
        return

    async with lock:
        msg = await update.message.reply_text(
            f"Стартую сбор:\n• URL: {url}\n• Объявлений: {count}\nЭто займёт немного времени…"
        )

        try:
            out_jsonl = getattr(avito_scraper, "OUTPUT_JSONL", Path("./data/avito_items.jsonl"))
            out_jsonl = Path(out_jsonl) if not isinstance(out_jsonl, Path) else out_jsonl
            out_jsonl.parent.mkdir(parents=True, exist_ok=True)

            # запускаем скрейпер (он async)
            await avito_scraper.run(url, count, skip_robots=True)

            payload = _jsonl_to_array_bytes(out_jsonl)
            ts = int(time.time())
            fname = f"avito_{chat_id}_{ts}.json"
            bio = io.BytesIO(payload)
            bio.name = fname

            await update.message.reply_document(document=InputFile(bio, filename=fname),
                                                caption="Готово ✅ Вот JSON.")

            try:
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

# ---------- entrypoint ----------
def main() -> None:
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scrape", scrape_cmd))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), any_text))

    if WEBHOOK_URL:
        # Блокирующий запуск webhook — без await
        app.run_webhook(
            listen=HOST,
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=WEBHOOK_URL,
            drop_pending_updates=True,
        )
    else:
        # Блокирующий запуск long polling — без await
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Avito category/search -> items scraper (Playwright, BS4, JSON Schema)
Перезапись JSONL на каждый запуск + SQLite-дедупликация обработанных URL.
Собирает именно НЕВИДЕННЫЕ ссылки (учитывая SQLite) до N штук.

Обновлено:
- Точный парсинг общего рейтинга продавца (.seller-info-rating .CiqtH или контекст вокруг [data-marker="sellerRate"])
- Точный парсинг рейтинга каждого отзыва через [data-marker="review(X)/score"]
- Подчищены эвристики отзывов и шумовые баннеры

Запуск:
  python avito_scraper.py --start-url "https://www.avito.ru/moskva/velosipedy?cd=1" --max-items 2
"""

import asyncio
import json
import os
import random
import re
import sys
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Callable
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from dotenv import load_dotenv

# -------------------- Env / Config --------------------
load_dotenv()

USER_AGENT = os.getenv(
    "SCRAPER_USER_AGENT",
    "IP-Analytics-AvitoScraper/0.2 (+contact: your-email@example.com)"
)
DEFAULT_HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"}
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
SLOW_MIN, SLOW_MAX = 0.7, 1.8
MAX_RETRIES = 3

MSK_TZ = timezone(timedelta(hours=3), name="Europe/Moscow")

DATA_DIR = Path("./data")
SNAP_DIR = Path("./snapshots")
LOG_DIR = Path("./logs")
HEADLESS = os.getenv("HEADLESS", "0").lower() not in ("0", "false", "no")
BLOCK_MEDIA = os.getenv("BLOCK_MEDIA", "0").lower() in ("1", "true", "yes")
CHROME_CHANNEL = os.getenv("CHROME_CHANNEL", "").strip()
CAPTCHA_MODE = os.getenv("CAPTCHA_MODE", "manual").lower()

for d in (DATA_DIR, SNAP_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

STORAGE_STATE = os.getenv("STORAGE_STATE", str(DATA_DIR / "state.json"))
OUTPUT_JSONL = DATA_DIR / "avito_items.jsonl"
DB_PATH = DATA_DIR / "seen_urls.sqlite3"   # SQLite с обработанными URL

# -------------------- JSON Schema --------------------
SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "AvitoItem",
    "type": "object",
    "additionalProperties": False,
    "required": ["name", "url", "price", "features", "description", "seller_rate", "seller_reviews"],
    "properties": {
        "name": {"type": "string"},
        "url": {"type": "string", "format": "uri"},
        "price": {"type": "number"},
        "features": {"type": "string"},
        "description": {"type": "string"},
        "seller_rate": {"type": "number"},
        "seller_reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["review", "rate"],
                "properties": {
                    "review": {"type": "string"},
                    "rate": {"type": "number"},
                },
            },
        },
    },
}
SCHEMA_VALIDATOR = Draft202012Validator(SCHEMA)

# -------------------- Utils --------------------
def jlog(level: str, msg: str, **kwargs):
    payload = {"ts": datetime.now(tz=timezone.utc).isoformat(), "level": level.upper(), "msg": msg, **kwargs}
    line = json.dumps(payload, ensure_ascii=False)
    print(line)
    with open(LOG_DIR / "run.jsonl", "a", encoding="utf-8") as f:
        f.write(line + "\n")


async def async_sleep_polite():
    await asyncio.sleep(random.uniform(SLOW_MIN, SLOW_MAX))


def price_to_number(text: str) -> Optional[float]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return float(digits) if digits else None


def flatten_text(elements: List[str]) -> str:
    parts = [re.sub(r"\s+", " ", t).strip() for t in elements if t and t.strip()]
    return "\n".join([p for p in parts if p])


def is_avito_firewall(html: str) -> bool:
    h = html.lower()
    return ("firewall-title" in h or "доступ ограничен" in h or "/web/1/firewallcaptcha/" in h
            or "js-firewall-form" in h or "geetest_captcha" in h or "h-captcha" in h)


def normalize_avito_url(u: str) -> str:
    if not u:
        return u
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin("https://www.avito.ru", u)
    sp = urlsplit(u)
    u = urlunsplit((sp.scheme, sp.netloc, sp.path, "", ""))  # обрезаем query и fragment
    u = re.sub(r"([?&])utm_[^=&]+=[^&]+", r"\1", u)
    u = re.sub(r"([?&])s=[^&]+", r"\1", u)
    return u.rstrip("?&").rstrip("#")


def is_item_url_strict(u: str) -> bool:
    """
    Карточка: /<city>/<category>/<slug>_<ID>
    Ровно 3 сегмента; последний заканчивается на _<7+ цифр>.
    """
    try:
        pu = urlparse(u)
        if pu.netloc not in {"avito.ru", "www.avito.ru", "m.avito.ru"}:
            return False
        parts = (pu.path or "/").strip("/").split("/")
        if len(parts) != 3:
            return False
        last = parts[-1]
        return bool(re.fullmatch(r".+_\d{7,}", last))
    except Exception:
        return False


def save_snapshot(html: str, name: str):
    p = SNAP_DIR / f"{name}.html"
    p.write_text(html, encoding="utf-8")
    return str(p)

# -------------------- SQLite (seen URLs) --------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS seen ("
        " url TEXT PRIMARY KEY,"
        " dt  TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    return conn

def db_is_seen(conn: sqlite3.Connection, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
    return cur.fetchone() is not None

def db_mark_seen(conn: sqlite3.Connection, url: str):
    try:
        conn.execute("INSERT OR IGNORE INTO seen(url) VALUES (?)", (url,))
        conn.commit()
    except Exception as e:
        jlog("WARN", "Не удалось пометить URL в БД", error=str(e), url=url)

# -------------------- Robots.txt --------------------
async def fetch_robots_allow(domain_root: str, paths: List[str], skip_check: bool = False) -> Tuple[bool, Dict[str, bool]]:
    if skip_check:
        return True, {p: True for p in paths}
    robots_url = domain_root.rstrip("/") + "/robots.txt"
    allowed = {}
    try:
        async with httpx.AsyncClient(headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT) as client:
            r = await client.get(robots_url, follow_redirects=True)
            content = r.text if r.status_code == 200 else ""
    except Exception as e:
        jlog("WARN", "Не удалось скачать robots.txt; по умолчанию останавливаемся", error=str(e), robots_url=robots_url)
        return False, {p: False for p in paths}

    groups, current, ua_any = [], [], False
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("user-agent"):
            if current:
                groups.append(current); current = []
            ua = line.split(":", 1)[1].strip()
            ua_any = (ua == "*")
        elif ua_any and (line.lower().startswith("disallow") or line.lower().startswith("allow")):
            current.append(line)
    if current:
        groups.append(current)

    disallows, allows = [], []
    for grp in groups:
        for rule in grp:
            k, v = [x.strip() for x in rule.split(":", 1)]
            (disallows if k.lower() == "disallow" else allows).append(v)

    def path_allowed(path: str) -> bool:
        dismatch = [d for d in disallows if d and path.startswith(d)]
        if not dismatch:
            return True
        allow_over = [a for a in allows if a and path.startswith(a)]
        return bool(allow_over and max(map(len, allow_over)) >= max(map(len, dismatch)))

    for p in paths:
        allowed[p] = path_allowed(p)
    return all(allowed.values()), allowed

# -------------------- Playwright helpers --------------------
async def launch_browser():
    pw = await async_playwright().start()
    launch_kwargs = {
        "headless": HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--use-gl=swiftshader", "--enable-webgl",
            "--no-sandbox",
        ],
    }
    # внутри launch_browser()

    browser = await (pw.chromium.launch(channel=CHROME_CHANNEL, **{
        "headless": HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--use-gl=swiftshader", "--enable-webgl",
            "--no-sandbox",
            "--disable-dev-shm-usage",  # <— добавили
        ],
    }) if CHROME_CHANNEL else pw.chromium.launch(
        headless=HEADLESS,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--use-gl=swiftshader", "--enable-webgl",
            "--no-sandbox",
            "--disable-dev-shm-usage",  # <— добавили
        ],
    ))

    context = await browser.new_context(
        storage_state=STORAGE_STATE,
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1280, "height": 900},
    )

    # щедрее таймауты
    context.set_default_navigation_timeout(90_000)
    context.set_default_timeout(60_000)
    await context.set_extra_http_headers({"Accept-Language": "ru-RU,ru;q=0.9"})

    if BLOCK_MEDIA:
        async def route_handler(route):
            if route.request.resource_type in {"media", "font"}:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_handler)

    return pw, browser, context


async def dismiss_overlays(page) -> None:
    try:
        for sel in [
            'button:has-text("Хорошо")',
            'button:has-text("Понятно")',
            'button:has-text("Принять")',
            'button:has-text("Да, верно")',
            'button:has-text("Сохранить")',
        ]:
            loc = page.locator(sel)
            if await loc.count() > 0:
                try:
                    await loc.first.click(timeout=1500)
                    await async_sleep_polite()
                except Exception:
                    pass
    except Exception:
        pass


def _derive_category_slug(start_url: str) -> str:
    try:
        u = urlparse(start_url)
        parts = (u.path or "/").strip("/").split("/")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
    except Exception:
        pass
    return ""


async def scroll_collect_unseen_links(
    page,
    max_unseen: int,
    start_url: str,
    is_seen: Optional[Callable[[str], bool]] = None
) -> List[str]:
    """Собирает до max_unseen НЕВИДЕННЫХ ссылок на карточки, продолжая скроллить пока это возможно."""
    seen_on_run: set = set()
    result: List[str] = []

    def want(url: str) -> bool:
        if not is_item_url_strict(url):
            return False
        if url in seen_on_run:
            return False
        if is_seen and is_seen(url):
            return False
        return True

    async def harvest() -> Tuple[int, int, int]:
        hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href') || '')")
        total, matched_all, matched_unseen = 0, 0, 0
        for h in hrefs:
            if not h:
                continue
            total += 1
            h = normalize_avito_url(h)
            if is_item_url_strict(h):
                matched_all += 1
                if want(h):
                    matched_unseen += 1
                    seen_on_run.add(h)
                    result.append(h)
        return total, matched_all, matched_unseen

    slug = _derive_category_slug(start_url)
    if slug:
        try:
            await page.wait_for_selector(f'a[href*="/{slug}/"]', timeout=15_000)
        except Exception:
            pass

    total, matched_all, _ = await harvest()
    jlog("INFO", "Диагностика ссылок (первый проход)", total_anchors=total, matched=matched_all)

    stagnant_rounds, last_len = 0, len(result)
    while len(result) < max_unseen and stagnant_rounds < 10:
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        await async_sleep_polite()
        total, matched_all, _ = await harvest()
        jlog("INFO", "Диагностика ссылок (скролл)", total_anchors=total, matched=matched_all, collected=len(result))
        if len(result) == last_len:
            stagnant_rounds += 1
        else:
            stagnant_rounds, last_len = 0, len(result)

    if not result:
        html = await page.content()
        save_snapshot(html, "category_zero_links")

    return result[:max_unseen]

# -------------------- Reviews helpers --------------------
RE_REV_WORD = re.compile(r"\bотзыв(?:ов|а)?\b", re.I)
EXCLUDE_SNIPPETS = ("Вы открыли объявление в Бизнес 360",)

async def _close_noise(page) -> None:
    """Прижимаем налипшие баннеры/тосты (Бизнес 360 и т.п.)."""
    try:
        xbtn = page.locator('button[aria-label*="Закрыть"], [role="button"]:has-text("Закрыть")')
        if await xbtn.count():
            try:
                await xbtn.first.click(timeout=800)
            except Exception:
                pass
        await page.evaluate("""() => {
          const bad = [
            '[data-marker*="business-360"]',
            '[class*="Business360"]', '[class*="b360"]',
            '[data-marker*="toast"]'
          ];
          for (const sel of bad) document.querySelectorAll(sel).forEach(n => n.remove());
          const sticky = [...document.querySelectorAll('div')].find(d => d.textContent && d.textContent.includes('Бизнес 360'));
          if (sticky) sticky.remove();
        }""")
    except Exception:
        pass

async def _open_reviews_ui(page) -> str:
    """
    Пытаемся открыть «Отзывы». Возвращаем:
    - 'dialog'  — открылась модалка;
    - 'page'    — перешли в профиль/вкладку «Отзывы»;
    - ''        — не удалось.
    """
    # 1) Ссылка/кнопка «Отзывы»
    try:
        loc = page.locator("a,button").filter(has_text=RE_REV_WORD)
        if await loc.count():
            try:
                await loc.first.click(timeout=3000, force=True)
            except Exception:
                try:
                    await loc.first.dblclick(timeout=3000, force=True)
                except Exception:
                    pass
            try:
                await page.wait_for_selector('text=/Отзывы о пользователе/i', timeout=5000)
                return "dialog"
            except Exception:
                pass
    except Exception:
        pass

    # 2) Профиль → вкладка «Отзывы»
    try:
        prof = page.locator('a[href*="/profile"], a[href*="/user/"], a:has-text("Профиль"), a:has-text("Пользователь")')
        if await prof.count():
            await prof.first.click(timeout=5000)
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
            tab = page.locator('a:has-text("Отзывы"), button:has-text("Отзывы")')
            if await tab.count():
                await tab.first.click(timeout=5000)
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                return "page"
    except Exception:
        pass
    return ""

def _num_0_5(s: str) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"([0-5](?:[.,]\d)?)\s*(?:из\s*5)?", s, flags=re.I)
    if not m:
        return None
    v = m.group(1).replace(",", ".")
    try:
        f = float(v);  return max(0.0, min(5.0, f))
    except Exception:
        return None

def _attrs_text_chain(node) -> str:
    parts = []
    for el in [node, *node.find_all(True, recursive=True)]:
        for a in ("aria-label", "title", "alt"):
            if el.has_attr(a) and el.get(a):
                parts.append(str(el.get(a)))
    return " | ".join(parts)

def _parse_reviews_from_html(html: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Сначала точный путь: ищем оценки через data-marker="review(X)/score".
    Если нет — включаем эвристики.
    """
    soup = BeautifulSoup(html, "lxml")
    reviews: List[Dict[str, Any]] = []
    seen = set()

    # 1) Точная разметка Avito
    star_nodes = soup.select('[data-marker^="review("][data-marker$="/score"]')
    for sn in star_nodes:
        dm = sn.get("data-marker", "")
        m = re.match(r"review\(([\d.,]+)\)/score", dm)
        rating = float(m.group(1).replace(",", ".")) if m else 0.0

        container = (sn.find_parent("article")
                     or sn.find_parent("li")
                     or sn.find_parent("section")
                     or sn.find_parent("div")
                     or sn)
        txt = container.get_text(" ", strip=True)
        txt = re.sub(r"Рейтинг\s+\d(\s|$)", " ", txt, flags=re.I)
        if not txt or len(txt) < 40:
            continue
        if any(s in txt for s in EXCLUDE_SNIPPETS):
            continue

        h = hash(txt)
        if h in seen:
            continue
        seen.add(h)
        reviews.append({"review": txt, "rate": float(max(0.0, min(5.0, rating)))})
        if len(reviews) >= limit:
            return reviews

    # 2) Эвристики
    containers = soup.select('[data-marker*="review"], [data-marker*="feedback"], article, section, li')
    for c in containers:
        if len(reviews) >= limit:
            break
        txt = c.get_text(" ", strip=True)
        if not txt or len(txt) < 40:
            continue
        if any(s in txt for s in EXCLUDE_SNIPPETS):
            continue
        if not ("Сделка состоялась" in txt
                or re.search(r"\b(Покупатель|Продавец)\b", txt, re.I)
                or re.search(r"\bиз\s*5\b", txt)):
            continue

        rating = None
        attrs_str = _attrs_text_chain(c)
        rating = _num_0_5(attrs_str)
        if rating is None:
            rating = _num_0_5(txt)
        if rating is None:
            star_nodes = c.select('[class*="star"], [aria-label*="звезд"], [title*="звезд"]')
            for sn in star_nodes:
                s = (sn.get("aria-label") or sn.get("title") or sn.get_text(" ", strip=True) or "")
                v = _num_0_5(s)
                if v is not None:
                    rating = v
                    break

        h = hash(txt)
        if h in seen:
            continue
        seen.add(h)
        reviews.append({"review": txt, "rate": float(rating or 0.0)})

    return reviews[:limit]

async def _collect_reviews(page, limit: int = 50) -> List[Dict[str, Any]]:
    await _close_noise(page)
    mode = await _open_reviews_ui(page)
    if not mode:
        return []

    reviews: List[Dict[str, Any]] = []
    stagnant = 0

    for _ in range(12):
        if mode == "dialog":
            html = await page.evaluate("""
                () => {
                  const dlg = document.querySelector('[role="dialog"]');
                  return dlg ? dlg.innerHTML : document.documentElement.innerHTML;
                }
            """)
        else:
            html = await page.content()

        batch = _parse_reviews_from_html(html, limit=limit-len(reviews))

        before = len(reviews)
        for b in batch:
            if b not in reviews:
                reviews.append(b)
                if len(reviews) >= limit:
                    break

        if len(reviews) >= limit:
            break

        stagnant = stagnant + 1 if len(reviews) == before else 0
        if stagnant >= 3:
            break

        try:
            if mode == "dialog":
                await page.evaluate("""
                  () => {
                    const dlg = document.querySelector('[role="dialog"]');
                    if (dlg) dlg.scrollTop = dlg.scrollHeight;
                  }
                """)
            else:
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            break

        await async_sleep_polite()

    return reviews

# -------------------- Rating helper (общий рейтинг продавца) --------------------
def _extract_seller_rating(soup: BeautifulSoup) -> float:
    """
    Точный парсинг рейтинга продавца:
    1) .seller-info-rating .CiqtH (например '5,0')
    2) Контекст вокруг [data-marker="sellerRate"]
    3) Fallback: JSON-LD / aria-label / 'из 5' / общий текст
    """
    el = soup.select_one('.seller-info-rating .CiqtH')
    if el:
        v = _num_0_5(el.get_text(strip=True))
        if v is not None:
            return float(v)

    sr = soup.select_one('[data-marker="sellerRate"]')
    if sr:
        parent = sr.parent or sr
        text_ctx = parent.get_text(" ", strip=True)
        v = _num_0_5(text_ctx)
        if v is not None:
            return float(v)

    # JSON-LD
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            txt = (node.get_text() or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if not isinstance(obj, dict):
                    continue
                if obj.get("@type") == "AggregateRating" and obj.get("ratingValue"):
                    v = _num_0_5(str(obj["ratingValue"]))
                    if v is not None:
                        return float(v)
                ag = obj.get("aggregateRating")
                if isinstance(ag, dict) and ag.get("ratingValue"):
                    v = _num_0_5(str(ag["ratingValue"]))
                    if v is not None:
                        return float(v)
    except Exception:
        pass

    for node in soup.select('[aria-label], [title], [alt]'):
        s = (node.get("aria-label") or node.get("title") or node.get("alt") or "").strip()
        if not s:
            continue
        if ("из 5" in s) or ("рейтинг" in s.lower()) or ("оцен" in s.lower()):
            v = _num_0_5(s)
            if v is not None:
                return float(v)

    v = _num_0_5(soup.get_text(" ", strip=True))
    return float(v) if v is not None else 0.0

# -------------------- Item extraction --------------------
async def extract_item_fields(page_url: str, context) -> Optional[Dict[str, Any]]:
    import html as htmllib

    clean_url = normalize_avito_url(page_url)
    page = await context.new_page()

    try:
        await page.goto(clean_url, wait_until="domcontentloaded", timeout=30_000)
    except PWTimeoutError:
        jlog("WARN", "Таймаут навигации к карточке", url=clean_url)
        await page.close(); return None
    except Exception as e:
        jlog("WARN", "Ошибка навигации к карточке", url=clean_url, error=str(e))
        await page.close(); return None

    try:
        await page.wait_for_selector(
            'meta[property="og:title"], [data-marker="item-view/title"], h1, '
            'h2.firewall-title, #geetest_captcha, #h-captcha',
            timeout=15_000
        )
    except Exception:
        jlog("WARN", "Селектор карточки не дождался — парсим как есть", url=clean_url)

    await page.wait_for_timeout(600)
    html = await page.content()

    if is_avito_firewall(html):
        jlog("WARN", "В карточке появилась капча/фаервол", url=clean_url)
        try:
            await page.bring_to_front()
        except Exception:
            pass
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: input(">>> В карточке есть капча. Пройдите её и нажмите Enter... "))
        try:
            await context.storage_state(path=STORAGE_STATE)
            jlog("INFO", "Состояние (cookies) сохранено после карточки", path=STORAGE_STATE)
        except Exception:
            pass
        await page.reload(wait_until="domcontentloaded")
        try:
            await page.wait_for_selector('meta[property="og:title"], h1', timeout=20_000)
        except Exception:
            pass
        html = await page.content()
        if is_avito_firewall(html):
            save_snapshot(html, f"captcha_{int(time.time()*1000)}")
            jlog("ERROR", "Капча в карточке не пройдена", url=clean_url)
            await page.close(); return None

    soup = BeautifulSoup(html, "lxml")

    canonical = soup.select_one('link[rel="canonical"]')
    canonical_url = canonical["href"].strip() if canonical and canonical.has_attr("href") else page.url

    name = ""
    mt = soup.find("meta", {"property": "og:title"})
    if mt and mt.get("content"):
        name = htmllib.unescape(mt["content"]).split("|", 1)[0].strip()
    if not name:
        tnode = soup.select_one('[data-marker="item-view/title"]') or soup.find("h1")
        if tnode:
            name = tnode.get_text(strip=True)

    price_num: Optional[float] = None
    mp = soup.find("meta", {"property": "product:price:amount"})
    if mp and mp.get("content"):
        price_num = price_to_number(mp["content"])
    if price_num is None:
        pnode = soup.select_one('[itemprop="price"], [data-marker="item-view/price"], meta[itemprop="price"]')
        if pnode:
            text = pnode.get("content") or pnode.get_text(" ", strip=True)
            price_num = price_to_number(text)
    if price_num is None:
        m = re.search(r"(\d[\d\s]{1,}\d)\s*₽", soup.get_text(" ", strip=True))
        if m:
            price_num = price_to_number(m.group(1))
    if price_num is None:
        price_num = 0.0

    description = ""
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            txt = (node.get_text() or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if isinstance(obj, dict) and obj.get("@type") == "Product" and obj.get("description"):
                    description = obj["description"]; break
            if description:
                break
    except Exception:
        pass
    if not description:
        el = soup.select_one('[data-marker="item-view/description"], div[itemprop="description"]')
        if el:
            description = re.sub(r"\s+", " ", el.get_text(" ", strip=True))

    features_texts: List[str] = []
    for cont in (soup.select('[data-marker*="item-params"]') or soup.select("ul.params-list") or []):
        items = cont.select("li")
        if items:
            for li in items:
                line = re.sub(r"\s+", " ", li.get_text(" ", strip=True))
                if line:
                    features_texts.append(line)
        else:
            txt = cont.get_text("\n", strip=True)
            if txt:
                features_texts.append(txt)
    features_flat = flatten_text(features_texts)

    seller_rate = _extract_seller_rating(soup)

    reviews: List[Dict[str, Any]] = []
    try:
        reviews = await _collect_reviews(page, limit=50)
    except Exception as e:
        jlog("WARN", "Ошибка при получении отзывов", url=page_url, error=str(e))

    record = {
        "name": (name or "").strip(),
        "url": canonical_url,
        "price": float(price_num or 0.0),
        "features": (features_flat or "").strip(),
        "description": (description or "").strip(),
        "seller_rate": float(seller_rate or 0.0),
        "seller_reviews": reviews or [],
    }

    errs = [e.message for e in sorted(SCHEMA_VALIDATOR.iter_errors(record), key=str)]
    if errs:
        jlog("ERROR", "Невалидная запись по схеме", url=clean_url, errors=errs)
        save_snapshot(html, f"invalid_item_{int(time.time())}")
        await page.close(); return None

    await page.close()
    return record
async def _open_category_or_fail(page, start_url: str, slug_hint: str, captcha_mode: str = "auto") -> bool:
    """
    Открывает страницу категории «по-умному»:
    - первый goto ждёт только commit (быстрый ответ сервера)
    - далее крутим цикл: проверяем firewall и наличие ссылок карточек
    Возвращает True, если категория готова; False — если не удалось.
    """
    # 1) быстрый заход
    try:
        await page.goto(start_url, wait_until="commit", timeout=45_000)
    except Exception as e:
        jlog("WARN", "Goto(commit) не успел", url=start_url, error=str(e))

    # 2) ждём контент каталога или firewall (до ~60с)
    t0 = time.time()
    last_err = ""
    while time.time() - t0 < 60:
        try:
            html = await page.content()
        except Exception as e:
            last_err = str(e)
            await asyncio.sleep(1.5)
            continue

        if "js-firewall-form" in html.lower() or "h-captcha" in html.lower() or "firewall-title" in html.lower():
            jlog("WARN", "Обнаружен firewall на категории")
            # если есть возможность — даём шанс вручную решить (если HEADLESS=0)
            if os.getenv("CAPTCHA_MODE", "manual").lower() == "manual" and (os.getenv("HEADLESS", "1") in ("0","false","no")):
                try:
                    await page.bring_to_front()
                except Exception:
                    pass
                jlog("WARN", "Решите капчу в окне браузера и вернитесь в консоль")
                # ждём, пока уйдёт firewall
                solved = False
                t1 = time.time()
                while time.time() - t1 < 180:
                    html2 = await page.content()
                    if not ("js-firewall-form" in html2.lower() or "h-captcha" in html2.lower() or "firewall-title" in html2.lower()):
                        solved = True
                        break
                    await asyncio.sleep(2)
                if not solved:
                    save_snapshot(html, "category_firewall_unsolved")
                    return False
                # иначе продолжаем цикл, чтобы дождаться контента
            else:
                save_snapshot(html, "category_firewall_headless")
                return False

        # контент каталога обычно даёт ссылки вида /<slug>/..._1234567
        if slug_hint:
            locator = page.locator(f'a[href*="/{slug_hint}/"]')
            try:
                if await locator.count() > 0:
                    return True
            except Exception:
                pass

        # как минимум, если на странице есть <a> с id-паттерном карточки — тоже ок
        anchors = re.findall(r'href="([^"]+_\d{7,})"', html)
        if anchors:
            return True

        await asyncio.sleep(1.2)

    # таймаут ожидания
    try:
        html = await page.content()
        save_snapshot(html, "category_open_timeout")
    except Exception:
        pass
    if last_err:
        jlog("ERROR", "Категория не открылась", error=last_err)
    return False
# -------------------- Main flow --------------------
async def run(start_url: str, max_items: int, skip_robots: bool = False):
    domain_root = "https://www.avito.ru"
    try:
        path_part = "/" + "/".join(start_url.split("/", 3)[3:]).split("?")[0]
        if path_part == "/":
            path_part = "/moskva/"
    except Exception:
        path_part = "/moskva/"

    ok, rules = await fetch_robots_allow(domain_root, [path_part], skip_check=skip_robots)
    if not ok:
        jlog("ERROR", "robots.txt запрещает путь или не прочитан. Останавливаемся.", rules=rules)
        print("ROBOTS_BLOCK", rules); return

    # 1) Подготовка БД
    conn = db_connect()

    # 2) Перезаписываем JSONL на каждый запуск
    OUTPUT_JSONL.write_text("", encoding="utf-8")
    jlog("INFO", "Файл результата обнулён", out=str(OUTPUT_JSONL))

    pw, browser, context = await launch_browser()
    page = await context.new_page()
    page.set_default_navigation_timeout(90_000)

    slug = _derive_category_slug(start_url)

    ok_open = await _open_category_or_fail(page, start_url, slug_hint=slug)
    if not ok_open:
        jlog("ERROR", "Категория не открылась (после commit-режима)", url=start_url)
        await browser.close();
        await pw.stop();
        conn.close();
        return

    await dismiss_overlays(page)

    html0 = (await page.content()).lower()
    if "js-firewall-form" in html0 or "h-captcha" in html0:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: input(">>> На категории/поиске есть капча. Пройдите и нажмите Enter... "))
        try:
            await context.storage_state(path=STORAGE_STATE)
            jlog("INFO", "Состояние (cookies) сохранено", path=STORAGE_STATE)
        except Exception:
            pass

    # Собираем именно НЕВИДЕННЫЕ ссылки (учитывая SQLite)
    links = await scroll_collect_unseen_links(
        page,
        max_unseen=max_items,
        start_url=start_url,
        is_seen=lambda u: db_is_seen(conn, normalize_avito_url(u))
    )

    jlog("INFO", "Собрали ссылки", total=len(links), unique=len(set(links)), skipped_by_db=0, links=links)

    results_count = 0
    with open(OUTPUT_JSONL, "a", encoding="utf-8") as out:
        for href in links:
            tries, rec = 0, None
            while tries < MAX_RETRIES:
                tries += 1
                rec = await extract_item_fields(href, context)
                if rec:
                    break
                backoff = min(6, 1.5 ** tries) + random.uniform(0.0, 0.6)
                jlog("WARN", "Повторная попытка карточки", url=href, attempt=tries, sleep=round(backoff, 2))
                await asyncio.sleep(backoff)
            if rec:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                results_count += 1
                db_mark_seen(conn, normalize_avito_url(rec.get("url") or href))
            else:
                jlog("ERROR", "Не удалось извлечь карточку после повторов", url=href)
            await async_sleep_polite()

    await browser.close(); await pw.stop(); conn.close()
    jlog("INFO", "Готово", extracted=results_count, out=str(OUTPUT_JSONL))

# -------------------- CLI --------------------
def parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Avito scraper (category/search -> items -> seller reviews)")
    p.add_argument("--start-url", required=True, help="Страница категории или поиска Avito")
    p.add_argument("--max-items", type=int, default=2, help="Сколько НЕВИДЕННЫХ карточек извлечь (по умолчанию 2)")
    p.add_argument("--skip-robots-check", action="store_true", help="Пропустить проверку robots.txt (не рекомендуется)")
    return p.parse_args(argv)

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    asyncio.run(run(args.start_url, args.max_items, skip_robots=args.skip_robots_check))

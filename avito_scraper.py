#!/usr/bin/env python3
"""
Avito category -> items scraper (Playwright, BS4, JSON Schema)

Обновления (важное):
- FIX: await context.set_extra_http_headers(...) — ушло предупреждение про "was never awaited".
- Надёжное открытие карточки: ждём og:title/h1 ИЛИ признаки капчи; если селектор не дождался — парсим то, что есть.
- Чистим ?context/anchor у URL карточки.
- Locator .all() заменён на count()/first — стабильнее в новых версиях Playwright.
"""

import asyncio
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup
from jsonschema import Draft202012Validator
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from dotenv import load_dotenv

# -------------------- Env / Config --------------------
load_dotenv()

USER_AGENT = os.getenv(
    "SCRAPER_USER_AGENT",
    "IP-Analytics-AvitoScraper/0.1 (+contact: your-email@example.com)"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}
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
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"

# -------------------- JSON Schema --------------------
SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/schemas/avito_item.schema.json",
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


async def async_sleep_polite():  # мелкие гуманные паузы
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


async def ensure_access_or_wait_manual(page, context) -> bool:
    html = (await page.content()).lower()
    if "доступ ограничен: проблема с ip" in html or "js-firewall-form" in html or "h-captcha" in html:
        if CAPTCHA_MODE != "manual":
            jlog("ERROR", "Обнаружен firewall/капча, а CAPTCHA_MODE != manual", mode=CAPTCHA_MODE)
            save_snapshot(await page.content(), "firewall_detected")
            return False
        jlog("WARN", "Обнаружен firewall/капча. Решите её в окне браузера и вернитесь в консоль.")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: input(">>> Пройдите капчу и нажмите Enter... "))
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
        except Exception:
            pass
        try:
            await context.storage_state(path=STORAGE_STATE)
            jlog("INFO", "Состояние (cookies) сохранено", path=STORAGE_STATE)
        except Exception as e:
            jlog("WARN", "Не удалось сохранить storage_state", error=str(e))
    return True


def validate_record(obj: Dict[str, Any]) -> List[str]:
    return [e.message for e in sorted(SCHEMA_VALIDATOR.iter_errors(obj), key=str)]


def save_snapshot(html: str, name: str):
    p = SNAP_DIR / f"{name}.html"
    p.write_text(html, encoding="utf-8")
    return str(p)


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
    ok = all(allowed.values())
    return ok, allowed


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
    browser = await (pw.chromium.launch(channel=CHROME_CHANNEL, **launch_kwargs)
                     if CHROME_CHANNEL else pw.chromium.launch(**launch_kwargs))

    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    context = await browser.new_context(
        storage_state=STORAGE_STATE,
        user_agent=UA,
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1280, "height": 900},
    )

    # эти — sync
    context.set_default_navigation_timeout(60_000)
    context.set_default_timeout(30_000)
    # а этот — async (ВАЖНО: await)
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
            locs = page.locator(sel)
            if await locs.count() > 0:
                try:
                    await locs.first.click(timeout=1500)
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


def _is_item_url(href: str, category_slug: str) -> bool:
    if not href:
        return False
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("/"):
        href = urljoin("https://www.avito.ru", href)
    pu = urlparse(href)
    if pu.netloc not in {"avito.ru", "www.avito.ru", "m.avito.ru"}:
        return False
    path = pu.path or "/"
    if category_slug and f"/{category_slug}/" not in (path if path.endswith("/") else path + "/"):
        return False
    return bool(re.search(r"(?:_|/)(\d{7,})(?:/?|\b)", path))


async def scroll_collect_links(page, max_links: int, start_url: str) -> List[str]:
    seen, links = set(), []
    category_slug = _derive_category_slug(start_url)

    async def harvest() -> Tuple[int, int]:
        hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href') || '')")
        total, matched = 0, 0
        for h in hrefs:
            if not h:
                continue
            total += 1
            if h.startswith("//"):
                h = "https:" + h
            if h.startswith("/"):
                h = urljoin("https://www.avito.ru", h)
            h = re.sub(r"([?&])utm_[^=&]+=[^&]+", r"\1", h)
            h = re.sub(r"([?&])s=[^&]+", r"\1", h)
            h = h.rstrip("?&").rstrip("#")
            if _is_item_url(h, category_slug):
                matched += 1
                if h not in seen:
                    seen.add(h)
                    links.append(h)
        return total, matched

    try:
        await page.wait_for_selector(f'a[href*="/{_derive_category_slug(start_url)}/"]', timeout=15000)
    except Exception:
        pass

    stagnant_rounds, last_len = 0, 0
    total, matched = await harvest()
    jlog("INFO", "Диагностика ссылок (первый проход)", total_anchors=total, matched=matched)

    while len(links) < max_links and stagnant_rounds < 6:
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        await async_sleep_polite()
        total, matched = await harvest()
        jlog("INFO", "Диагностика ссылок (скролл)", total_anchors=total, matched=matched, collected=len(links))
        if len(links) == last_len:
            stagnant_rounds += 1
        else:
            stagnant_rounds, last_len = 0, len(links)

    if not links:
        html = await page.content()
        save_snapshot(html, "category_zero_links")

    return links[:max_links]


# -------------------- Item extraction --------------------
async def extract_item_fields(page_url: str, context) -> Optional[Dict[str, Any]]:
    import html as htmllib

    clean_url = re.sub(r"[?#].*$", "", page_url)  # убираем ?context и хвосты
    page = await context.new_page()

    # 1) навигация
    try:
        await page.goto(clean_url, wait_until="domcontentloaded", timeout=30_000)
    except PWTimeoutError:
        jlog("WARN", "Таймаут навигации к карточке", url=clean_url)
        await page.close()
        return None
    except Exception as e:
        jlog("WARN", "Ошибка навигации к карточке", url=clean_url, error=str(e))
        await page.close()
        return None

    # 2) ждём «признак жизни» страницы (title/og:title/h1) либо капчу
    try:
        await page.wait_for_selector(
            'meta[property="og:title"], [data-marker="item-view/title"], h1, '
            'h2.firewall-title, #geetest_captcha, #h-captcha',
            timeout=15_000
        )
    except Exception:
        jlog("WARN", "Селектор карточки не дождался — парсим как есть", url=clean_url)

    await page.wait_for_timeout(800)
    html = await page.content()

    # 3) капча?
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
            await page.close()
            return None

    # 4) парсим
    soup = BeautifulSoup(html, "lxml")

    # canonical
    canonical = soup.select_one('link[rel="canonical"]')
    canonical_url = canonical["href"].strip() if canonical and canonical.has_attr("href") else page.url

    # name
    name = ""
    mt = soup.find("meta", {"property": "og:title"})
    if mt and mt.get("content"):
        name = htmllib.unescape(mt["content"]).split("|", 1)[0].strip()
    if not name:
        tnode = soup.select_one('[data-marker="item-view/title"]') or soup.find("h1")
        if tnode:
            name = tnode.get_text(strip=True)

    # price
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
        price_num = 0.0

    # description
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

    # features (плоско)
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

    # seller_rate
    seller_rate = 0.0
    rating_text = ""
    cand = soup.find_all(string=re.compile(r"\b[0-5](?:[.,]\d)?\s*из\s*5\b"))
    if cand:
        rating_text = str(cand[0])
    else:
        star = soup.select_one('[aria-label*="из 5"]')
        if star and star.has_attr("aria-label"):
            rating_text = star["aria-label"]
    if rating_text:
        m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", rating_text)
        if m:
            seller_rate = float(m.group(1).replace(",", "."))

    # reviews
    reviews: List[Dict[str, Any]] = []
    try:
        loc = page.locator('a:has-text("Отзывы"), button:has-text("Отзывы")')
        if await loc.count() > 0:
            try:
                await loc.first.click(timeout=3000)
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                await async_sleep_polite()
            except Exception:
                pass

        reviews = await _extract_reviews_from_page(page, limit=50)

        if len(reviews) < 5:
            seller_links = page.locator('a:has-text("Профиль"), a[href*="user"], a:has-text("Все отзывы")')
            if await seller_links.count() > 0:
                try:
                    await seller_links.first.click(timeout=3000)
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    await async_sleep_polite()
                    tab = page.locator('a:has-text("Отзывы"), button:has-text("Отзывы")')
                    if await tab.count() > 0:
                        try:
                            await tab.first.click(timeout=3000)
                            await page.wait_for_load_state("domcontentloaded", timeout=10000)
                            await async_sleep_polite()
                        except Exception:
                            pass
                    reviews = await _extract_reviews_from_page(page, limit=50)
                except Exception:
                    pass
    except Exception as e:
        jlog("WARN", "Ошибка при получении отзывов", url=clean_url, error=str(e))

    record = {
        "name": (name or "").strip(),
        "url": canonical_url,
        "price": float(price_num or 0.0),
        "features": (features_flat or "").strip(),
        "description": (description or "").strip(),
        "seller_rate": float(seller_rate or 0.0),
        "seller_reviews": reviews or [],
    }

    errs = validate_record(record)
    if errs:
        jlog("ERROR", "Невалидная запись по схеме", url=clean_url, errors=errs)
        save_snapshot(html, f"invalid_item_{int(time.time())}")
        await page.close()
        return None

    await page.close()
    return record


async def _extract_reviews_from_page(page, limit: int = 50) -> List[Dict[str, Any]]:
    reviews: List[Dict[str, Any]] = []
    seen = set()

    async def pull() -> List[Dict[str, Any]]:
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select('[data-marker*="review"], [class*="review"], article')
        out = []
        for c in cards:
            txt = c.get_text(" ", strip=True)
            if not txt or len(txt) < 10:
                continue
            rating = None
            star = c.select_one('[aria-label*="из 5"]')
            if star and star.has_attr("aria-label"):
                m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", star["aria-label"])
                if m: rating = float(m.group(1).replace(",", "."))
            if rating is None:
                m = re.search(r"([0-5])\s*из\s*5", txt)
                if m: rating = float(m.group(1))
            h = hash(txt)
            if h not in seen:
                seen.add(h)
                out.append({"review": txt, "rate": float(rating or 0.0)})
            if len(out) >= limit:
                break
        return out

    stagnant, last = 0, 0
    while len(reviews) < limit and stagnant < 4:
        batch = await pull()
        for b in batch:
            if b not in reviews:
                reviews.append(b)
                if len(reviews) >= limit:
                    break
        if len(reviews) >= limit:
            break
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await async_sleep_polite()
        except Exception:
            break
        stagnant = stagnant + 1 if len(reviews) == last else 0
        last = len(reviews)

    return reviews[:limit]


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
        print("ROBOTS_BLOCK", rules)
        return

    pw, browser, context = await launch_browser()
    page = await context.new_page()
    page.set_default_navigation_timeout(60_000)

    try:
        await page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        jlog("ERROR", "Не удалось открыть страницу категории", url=start_url, error=str(e))
        await browser.close(); await pw.stop()
        return

    await dismiss_overlays(page)
    if not await ensure_access_or_wait_manual(page, context):
        await browser.close(); await pw.stop(); return

    try:
        await page.wait_for_selector(f'a[href*="/{_derive_category_slug(start_url)}/"]', timeout=20_000)
    except Exception:
        pass
    await async_sleep_polite()

    links = await scroll_collect_links(page, max_items, start_url)
    jlog("INFO", "Собрали ссылки", total=len(links), links=links)

    done = set()
    results: List[Dict[str, Any]] = []
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
            results.append(rec)
            with open(OUTPUT_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            done.add(href)
        else:
            jlog("ERROR", "Не удалось извлечь карточку после повторов", url=href)
        await async_sleep_polite()

    await browser.close(); await pw.stop()
    jlog("INFO", "Готово", extracted=len(results), out=str(OUTPUT_JSONL))


# -------------------- CLI --------------------
def parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Avito scraper (category -> items -> seller reviews)")
    p.add_argument("--start-url", required=True, help="URL страницы категории Avito (пример: https://www.avito.ru/moskva/velosipedy?cd=1)")
    p.add_argument("--max-items", type=int, default=2, help="Сколько карточек извлечь (по умолчанию 2)")
    p.add_argument("--skip-robots-check", action="store_true", help="Пропустить проверку robots.txt (не рекомендуется)")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    asyncio.run(run(args.start_url, args.max_items, skip_robots=args.skip_robots_check))

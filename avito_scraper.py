#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ЛОКАЛЬНАЯ ВЕРСИЯ: Avito category/search -> items scraper (Playwright + BS4)
— Без прокси, без SQLite, без .env (всё с дефолтами)
— HEADLESS=0 по умолчанию: откроется окно браузера; если будет капча/фаервол,
  просто решите её в этом окне — скрипт подождёт.

Функция scrape(start_url, max_items, out_path=None) -> str
возвращает путь к JSON-файлу с массивом объектов.

CLI:
  python avito_scraper.py --start-url "https://www.avito.ru/moskva/velosipedy?cd=1" --max-items 2
"""

import asyncio
import json
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

# -------------------- Простые настройки --------------------
DATA_DIR = Path("./data")
SNAP_DIR = Path("./snapshots")
EXPORTS_DIR = Path("./exports")
for d in (DATA_DIR, SNAP_DIR, EXPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# На локалке хотим окно браузера
HEADLESS = os.getenv("HEADLESS", "0").lower() not in ("0", "false", "no")
# Вежливые задержки
SLOW_MIN, SLOW_MAX = 0.6, 1.4
MAX_RETRIES = 3

def _now_ts() -> str:
    return datetime.now(tz=timezone.utc).isoformat()

def _log(level: str, msg: str, **kw):
    payload = {"ts": _now_ts(), "level": level, "msg": msg, **kw}
    print(json.dumps(payload, ensure_ascii=False))

async def _nap():
    await asyncio.sleep(random.uniform(SLOW_MIN, SLOW_MAX))

# -------------------- Утилиты --------------------
def normalize_avito_url(u: str) -> str:
    if not u:
        return u
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin("https://www.avito.ru", u)
    sp = urlsplit(u)
    u = urlunsplit((sp.scheme, sp.netloc, sp.path, "", ""))
    u = re.sub(r"([?&])utm_[^=&]+=[^&]+", r"\1", u)
    u = re.sub(r"([?&])s=[^&]+", r"\1", u)
    return u.rstrip("?&").rstrip("#")

def is_item_url_strict(u: str) -> bool:
    """
    Карточка: /<city>/<category>/<slug>_<ID>
    Ровно 3 сегмента, последний заканчивается на _<7+ цифр>
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

def price_to_number(text: str) -> Optional[float]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return float(digits) if digits else None

def flatten_text(chunks: List[str]) -> str:
    parts = [re.sub(r"\s+", " ", t).strip() for t in chunks if t and t.strip()]
    return "\n".join([p for p in parts if p])

def save_snapshot(html: str, name: str) -> str:
    p = SNAP_DIR / f"{name}.html"
    p.write_text(html, encoding="utf-8")
    return str(p)

def is_avito_firewall(html: str) -> bool:
    h = html.lower()
    return (
        "firewall-title" in h
        or "доступ ограничен" in h
        or "/web/1/firewallcaptcha/" in h
        or "js-firewall-form" in h
        or "geetest_captcha" in h
        or "h-captcha" in h
    )

# -------------------- Playwright --------------------
async def launch_browser():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=HEADLESS,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--use-gl=swiftshader",
            "--enable-webgl",
            "--no-sandbox",
        ],
    )
    context = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1280, "height": 900},
    )
    context.set_default_navigation_timeout(60_000)
    context.set_default_timeout(30_000)
    await context.set_extra_http_headers({"Accept-Language": "ru-RU,ru;q=0.9"})
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
            if await loc.count():
                try:
                    await loc.first.click(timeout=1500)
                    await _nap()
                except Exception:
                    pass
    except Exception:
        pass

async def wait_firewall_solved(page, timeout_ms: int = 180_000) -> bool:
    """
    Ждём, пока пользователь решит капчу в открытом окне (HEADLESS=0).
    Проверяем DOM каждые 2 сек до timeout_ms. True — капча ушла.
    """
    start = time.time()
    try:
        await page.bring_to_front()
    except Exception:
        pass
    _log("WARN", "Обнаружен фаервол/капча. Решите её в окне браузера.")
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            html = await page.content()
            if not is_avito_firewall(html):
                return True
        except Exception:
            pass
        await asyncio.sleep(2.0)
    return False

# -------------------- Сбор ссылок --------------------
async def _derive_category_slug(start_url: str) -> str:
    try:
        u = urlparse(start_url)
        parts = (u.path or "/").strip("/").split("/")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
    except Exception:
        pass
    return ""

async def collect_links(page, max_links: int, start_url: str) -> List[str]:
    seen, links = set(), []

    async def harvest() -> Tuple[int, int]:
        hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href') || '')")
        total, matched = 0, 0
        for h in hrefs:
            if not h:
                continue
            total += 1
            h = normalize_avito_url(h)
            if is_item_url_strict(h) and h not in seen:
                matched += 1
                seen.add(h)
                links.append(h)
        return total, matched

    slug = await _derive_category_slug(start_url)
    if slug:
        try:
            await page.wait_for_selector(f'a[href*="/{slug}/"]', timeout=15_000)
        except Exception:
            pass

    total, matched = await harvest()
    _log("INFO", "Диагностика ссылок (первый проход)", total_anchors=total, matched=matched)

    stagnant, last_len = 0, len(links)
    while len(links) < max_links and stagnant < 8:
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        await _nap()
        total, matched = await harvest()
        _log("INFO", "Диагностика ссылок (скролл)", total_anchors=total, matched=matched, collected=len(links))
        if len(links) == last_len:
            stagnant += 1
        else:
            stagnant, last_len = 0, len(links)

    if not links:
        html = await page.content()
        save_snapshot(html, "category_zero_links")

    return links[:max_links]

# -------------------- Парсинг карточки --------------------
def _extract_rating(soup: BeautifulSoup) -> float:
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            txt = (node.get_text() or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if isinstance(obj, dict):
                    if obj.get("@type") == "AggregateRating" and obj.get("ratingValue"):
                        v = str(obj["ratingValue"]).replace(",", ".")
                        return float(re.sub(r"[^\d.]", "", v))
                    if "aggregateRating" in obj and isinstance(obj["aggregateRating"], dict):
                        v = str(obj["aggregateRating"].get("ratingValue", "")).replace(",", ".")
                        if v:
                            return float(re.sub(r"[^\d.]", "", v))
    except Exception:
        pass

    for el in soup.select('[aria-label*="из 5"], [title*="из 5"]'):
        src = el.get("aria-label") or el.get("title") or ""
        m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", src)
        if m:
            return float(m.group(1).replace(",", "."))
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", txt)
    return float(m.group(1).replace(",", ".")) if m else 0.0

async def _open_reviews_if_any(page) -> str:
    """
    Пытаемся открыть «Отзывы» (модалка или вкладка профиля).
    Возвращаем "dialog" | "page" | "".
    """
    # Кнопка/ссылка «Отзывы»
    try:
        loc = page.locator("a,button", has_text=re.compile(r"отзыв", re.I))
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

    # Профиль -> вкладка «Отзывы»
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

def _parse_reviews_from_html(html: str, limit: int = 50) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict[str, Any]] = []
    seen = set()
    for c in soup.select("article, li, div"):
        txt = c.get_text(" ", strip=True)
        if not txt or len(txt) < 60:
            continue
        if "Бизнес 360" in txt or "Вы открыли объявление" in txt:
            continue
        if not ("Сделка состоялась" in txt or "Покупатель" in txt or "Продавец" in txt or re.search(r"\bиз\s*5\b", txt)):
            continue
        rating = 0.0
        lab = c.select_one('[aria-label*="из 5"], [title*="из 5"]')
        if lab:
            src = lab.get("aria-label") or lab.get("title") or ""
            m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", src)
            if m:
                rating = float(m.group(1).replace(",", "."))
        if rating == 0.0:
            m = re.search(r"([0-5](?:[.,]\d)?)\s*из\s*5", txt)
            if m:
                rating = float(m.group(1).replace(",", "."))
        h = hash(txt)
        if h in seen:
            continue
        seen.add(h)
        out.append({"review": txt, "rate": float(rating)})
        if len(out) >= limit:
            break
    return out

async def _collect_reviews(page, mode: str, limit: int = 50) -> List[Dict[str, Any]]:
    reviews: List[Dict[str, Any]] = []
    stagnant = 0
    for _ in range(10):
        if mode == "dialog":
            html = await page.evaluate("""() => {
              const dlg = document.querySelector('[role="dialog"]');
              return dlg ? dlg.innerHTML : document.documentElement.innerHTML;
            }""")
        else:
            html = await page.content()
        batch = _parse_reviews_from_html(html, limit=limit - len(reviews))
        before = len(reviews)
        for b in batch:
            if b not in reviews:
                reviews.append(b)
                if len(reviews) >= limit:
                    break
        if len(reviews) >= limit:
            break
        stagnant = stagnant + 1 if len(reviews) == before else 0
        try:
            if mode == "dialog":
                await page.evaluate("""() => {
                  const dlg = document.querySelector('[role="dialog"]');
                  if (dlg) dlg.scrollTop = dlg.scrollHeight;
                }""")
            else:
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            break
        await _nap()
        if stagnant >= 3:
            break
    return reviews

async def extract_item(page_url: str, context) -> Optional[Dict[str, Any]]:
    import html as htmllib

    url = normalize_avito_url(page_url)
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    except PWTimeoutError:
        _log("WARN", "Таймаут навигации к карточке", url=url)
        await page.close(); return None
    except Exception as e:
        _log("WARN", "Ошибка навигации к карточке", url=url, error=str(e))
        await page.close(); return None

    # ждём что-то осмысленное (title/meta) ИЛИ firewall
    try:
        await page.wait_for_selector(
            'meta[property="og:title"], [data-marker="item-view/title"], h1, '
            'h2.firewall-title, #geetest_captcha, #h-captcha',
            timeout=15_000
        )
    except Exception:
        _log("WARN", "Селектор карточки не дождался — парсим как есть", url=url)

    await page.wait_for_timeout(600)
    html = await page.content()
    if is_avito_firewall(html):
        solved = await wait_firewall_solved(page, timeout_ms=180_000)
        if not solved:
            save_snapshot(html, f"captcha_{int(time.time()*1000)}")
            _log("ERROR", "Капча не решена вовремя", url=url)
            await page.close(); return None
        html = await page.content()

    soup = BeautifulSoup(html, "lxml")
    canonical = soup.select_one('link[rel="canonical"]')
    canonical_url = canonical["href"].strip() if canonical and canonical.has_attr("href") else page.url

    # title
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
        m = re.search(r"(\d[\d\s]{1,}\d)\s*₽", soup.get_text(" ", strip=True))
        if m:
            price_num = price_to_number(m.group(1))
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

    # features
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

    seller_rate = _extract_rating(soup)

    # отзывы (по возможности)
    reviews: List[Dict[str, Any]] = []
    try:
        mode = await _open_reviews_if_any(page)
        if mode:
            reviews = await _collect_reviews(page, mode=mode, limit=50)
    except Exception:
        pass

    await page.close()
    return {
        "name": (name or "").strip(),
        "url": canonical_url,
        "price": float(price_num or 0.0),
        "features": (features_flat or "").strip(),
        "description": (description or "").strip(),
        "seller_rate": float(seller_rate or 0.0),
        "seller_reviews": reviews or [],
    }

# -------------------- Публичный API --------------------
async def scrape(start_url: str, max_items: int, out_path: Optional[str] = None) -> str:
    pw, browser, context = await launch_browser()
    page = await context.new_page()
    page.set_default_navigation_timeout(60_000)

    try:
        await page.goto(start_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        _log("ERROR", "Не удалось открыть страницу категории/поиска", url=start_url, error=str(e))
        await browser.close(); await pw.stop()
        raise

    await dismiss_overlays(page)

    # если встретили firewall на категории — ждём ручного решения (окно открыто)
    html0 = (await page.content())
    if is_avito_firewall(html0):
        solved = await wait_firewall_solved(page, timeout_ms=180_000)
        if not solved:
            save_snapshot(html0, f"captcha_category_{int(time.time()*1000)}")
            _log("ERROR", "Капча на категории не решена")
            await browser.close(); await pw.stop()
            raise RuntimeError("Firewall/CAPTCHA on category not solved")

    links = await collect_links(page, max_items, start_url)
    _log("INFO", "Собрали ссылки", total=len(links), links=links)

    results: List[Dict[str, Any]] = []
    for href in links:
        tries = 0
        rec = None
        while tries < MAX_RETRIES:
            tries += 1
            rec = await extract_item(href, context)
            if rec:
                break
            await asyncio.sleep(min(6, 1.5 ** tries) + random.uniform(0, 0.6))
        if rec:
            results.append(rec)
        else:
            _log("ERROR", "Не удалось извлечь карточку после повторов", url=href)
        await _nap()

    await browser.close(); await pw.stop()

    if not out_path:
        out_path = str(EXPORTS_DIR / f"avito_{int(time.time())}.json")
    Path(out_path).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    _log("INFO", "JSON готов", out=out_path, items=len(results))
    return out_path

# -------------------- CLI для ручного теста --------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start-url", required=True)
    p.add_argument("--max-items", type=int, default=2)
    p.add_argument("--out", default="")
    args = p.parse_args()
    asyncio.run(scrape(args.start_url, args.max_items, args.out or None))

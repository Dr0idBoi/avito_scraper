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
from datetime import datetime, timezone
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

def _num_0_5(s: str) -> Optional[float]:
    """Достаём число 0..5 из строки (поддержка '5,0', '4.5', '5 из 5')."""
    if not s:
        return None
    m = re.search(r"([0-5](?:[.,]\d)?)\s*(?:из\s*5)?", s, flags=re.I)
    if not m:
        return None
    v = m.group(1).replace(",", ".")
    try:
        f = float(v)
        return max(0.0, min(5.0, f))
    except Exception:
        return None

def _attrs_text_chain(node) -> str:
    """Конкатенация aria-label/title/alt для узла и его потомков."""
    parts = []
    for el in [node, *node.find_all(True, recursive=True)]:
        for a in ("aria-label", "title", "alt"):
            if el.has_attr(a) and el.get(a):
                parts.append(str(el.get(a)))
    return " | ".join(parts)

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

async def wait_firewall_solved(page, timeout_ms: int = 180_000) -> bool:
    """Ждём, пока вы решите капчу в открытом окне (HEADLESS=0)."""
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
def _derive_category_slug(start_url: str) -> str:
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

    slug = _derive_category_slug(start_url)
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

# -------------------- Рейтинг продавца --------------------
def _extract_seller_rating(soup: BeautifulSoup) -> float:
    """
    Точный парсинг рейтинга продавца.
    1) Сначала верстка Avito: .seller-info-rating .CiqtH -> '5,0'
    2) Или ищем блок [data-marker="sellerRate"] и берём ближайшее число 0..5 в родительском тексте.
    3) Fallback: JSON-LD / aria-label / 'из 5' / общий текст.
    """

    # 1) Явный блок рядом со звёздами
    el = soup.select_one('.seller-info-rating .CiqtH')
    if el:
        v = _num_0_5(el.get_text(strip=True))
        if v is not None:
            return float(v)

    # 2) От блока sellerRate вверх/вокруг
    sr = soup.select_one('[data-marker="sellerRate"]')
    if sr:
        # ближайший видимый контекст
        parent = sr.parent or sr
        text_ctx = parent.get_text(" ", strip=True)
        v = _num_0_5(text_ctx)
        if v is not None:
            return float(v)

    # 3) Прежние общие эвристики
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

# -------------------- Отзывы: разметка и парсинг --------------------
EXCLUDE_SNIPPETS = ("Вы открыли объявление в Бизнес 360",)

def _parse_reviews_from_html(html: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Идём от реальных звёздочек отзыва:
      <div data-marker="review(5)/score"> … </div>
    Число берём из data-marker (review(5)), текст — из ближайшего большого контейнера.
    Если такой разметки нет, включаются старые эвристики.
    """
    soup = BeautifulSoup(html, "lxml")
    reviews: List[Dict[str, Any]] = []
    seen = set()

    # 1) Точный вариант по data-marker
    star_nodes = soup.select('[data-marker^="review("][data-marker$="/score"]')
    for sn in star_nodes:
        dm = sn.get("data-marker", "")
        m = re.match(r"review\(([\d.,]+)\)/score", dm)
        rating = float(m.group(1).replace(",", ".")) if m else 0.0

        # берём крупный контейнер с текстом вокруг звёзд
        container = (sn.find_parent("article")
                     or sn.find_parent("li")
                     or sn.find_parent("section")
                     or sn.find_parent("div")
                     or sn)

        txt = container.get_text(" ", strip=True)
        # подсчищаем технические подписи типа «Рейтинг 1», «Рейтинг 2» и т.п.
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

    # 2) Fallback: старые эвристики (на случай другой вёрстки)
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

async def _open_reviews_if_any(page) -> str:
    """
    Пытаемся открыть «Отзывы» (модалка или вкладка профиля).
    Возвращаем "dialog" | "page" | "".
    """
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

async def _collect_reviews(page, mode: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Собираем отзывы из открытой модалки или страницы профиля (mode='dialog'|'page')."""
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

        await _nap()

    return reviews

# -------------------- Парсинг карточки --------------------
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

    # общий рейтинг продавца — точный
    seller_rate = _extract_seller_rating(soup)

    # отзывы (по возможности)
    reviews: List[Dict[str, Any]] = []
    try:
        await _close_noise(page)
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

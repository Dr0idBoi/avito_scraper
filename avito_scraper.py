#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Avito category/search -> items scraper (Playwright + BS4)
— Серверная версия: читает настройки из .env, поддерживает прокси
— Авто-обход robots.txt (если недоступен/403 — продолжаем)
— Устойчивое открытие страниц (commit→ожидания, снапшоты при проблемах)
— Рейтинг продавца и баллы каждого отзыва приводятся к числу (0..5)

Асинхронный API:
  await run(start_url: str, max_items: int, skip_robots: bool = True)

Результат:
  JSONL в data/avito_items.jsonl (перезаписывается на каждый запуск)
"""

import os
import re
import json
import time
import asyncio
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

# -------------------- Директории/константы --------------------
DATA_DIR = Path("./data")
SNAP_DIR = Path("./snapshots")
for d in (DATA_DIR, SNAP_DIR):
    d.mkdir(parents=True, exist_ok=True)

OUTPUT_JSONL = DATA_DIR / "avito_items.jsonl"

HEADLESS = os.getenv("HEADLESS", "1").lower() in ("1", "true", "yes")
BLOCK_MEDIA = os.getenv("BLOCK_MEDIA", "1").lower() in ("1", "true", "yes")
STORAGE_STATE = os.getenv("STORAGE_STATE", str(DATA_DIR / "state.json"))
PROXY_URL = os.getenv("PROXY_URL", "").strip()

SLOW_MIN, SLOW_MAX = 0.6, 1.6
MAX_RETRIES = 3

# -------------------- Логирование --------------------
def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()

def jlog(level: str, msg: str, **kw):
    payload = {"ts": _now(), "level": level.upper(), "msg": msg, **kw}
    print(json.dumps(payload, ensure_ascii=False))

# -------------------- Утилиты --------------------
def _nap():
    return asyncio.sleep(random.uniform(SLOW_MIN, SLOW_MAX))

def normalize_url(u: str) -> str:
    if not u:
        return u
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin("https://www.avito.ru", u)
    sp = urlsplit(u)
    u = urlunsplit((sp.scheme, sp.netloc, sp.path, "", ""))  # убираем query/fragment
    u = re.sub(r"([?&])utm_[^=&]+=[^&]+", r"\1", u)
    u = re.sub(r"([?&])s=[^&]+", r"\1", u)
    return u.rstrip("?&").rstrip("#")

def is_item_url(u: str) -> bool:
    """Карточка: /<city>/<category>/<slug>_<ID> — ровно 3 сегмента, ID ≥7 цифр."""
    try:
        pu = urlparse(u)
        if pu.netloc not in {"avito.ru", "www.avito.ru", "m.avito.ru"}:
            return False
        parts = (pu.path or "/").strip("/").split("/")
        if len(parts) != 3:
            return False
        return bool(re.fullmatch(r".+_\d{7,}", parts[-1]))
    except Exception:
        return False

def price_to_number(text: str) -> float:
    if not text:
        return 0.0
    digits = re.sub(r"[^\d]", "", text)
    return float(digits) if digits else 0.0

def save_snapshot(html: str, name: str):
    p = SNAP_DIR / f"{name}.html"
    p.write_text(html or "", encoding="utf-8")
    return str(p)

def is_firewall(html: str) -> bool:
    if not html:
        return False
    h = html.lower()
    return any(x in h for x in (
        "firewall-title", "доступ ограничен", "/web/1/firewallcaptcha/",
        "js-firewall-form", "geetest_captcha", "h-captcha"
    ))

# -------------------- Прокси --------------------
def _playwright_proxy() -> Optional[Dict[str, str]]:
    """Готовим dict для Playwright, если PROXY_URL задан."""
    if not PROXY_URL:
        return None
    # Playwright понимает server + optional username/password
    # Разберём вручную:
    # schema://user:pass@host:port
    try:
        from urllib.parse import urlparse
        pu = urlparse(PROXY_URL)
        server = f"{pu.scheme}://{pu.hostname}:{pu.port}"
        out = {"server": server}
        if pu.username:
            out["username"] = pu.username
        if pu.password:
            out["password"] = pu.password
        return out
    except Exception:
        # как fallback отдадим «как есть»
        return {"server": PROXY_URL}

def _httpx_client() -> httpx.AsyncClient:
    proxies = None
    if PROXY_URL:
        proxies = {"http://": PROXY_URL, "https://": PROXY_URL}
    return httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ru-RU,ru;q=0.9"},
        timeout=20.0,
        proxies=proxies,
        follow_redirects=True,
        verify=True,
    )

# -------------------- Robots (мягкая проверка) --------------------
async def robots_soft_allow(start_url: str) -> bool:
    """
    Пытаемся скачать robots.txt. Если 403/ошибка — не блокируем (возвращаем True),
    просто логируем предупреждение. Это защищает от «ложной» остановки.
    """
    try:
        pu = urlparse(start_url)
        robots_url = f"{pu.scheme}://{pu.netloc}/robots.txt"
        async with _httpx_client() as client:
            r = await client.get(robots_url)
        if r.status_code != 200:
            jlog("WARN", "robots.txt недоступен — продолжаем", status=r.status_code, robots_url=robots_url)
            return True
        txt = r.text
        path = "/" + "/".join(start_url.split("/", 3)[3:]).split("?")[0]
        if not path:
            return True
        # Грубая проверка: ищем явный Disallow для начала пути
        dis = []
        allow = []
        ua_any = False
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("user-agent"):
                ua = line.split(":", 1)[1].strip()
                ua_any = (ua == "*")
            elif ua_any and line.lower().startswith("disallow"):
                dis.append(line.split(":", 1)[1].strip())
            elif ua_any and line.lower().startswith("allow"):
                allow.append(line.split(":", 1)[1].strip())
        deny = [d for d in dis if d and path.startswith(d)]
        if not deny:
            return True
        allow_over = [a for a in allow if a and path.startswith(a)]
        ok = bool(allow_over and max(map(len, allow_over)) >= max(map(len, deny)))
        if not ok:
            jlog("WARN", "robots.txt: путь в зоне Disallow — продолжаем на свой риск", path=path)
        return True  # всё равно мягко разрешаем
    except Exception as e:
        jlog("WARN", "robots.txt: ошибка чтения — продолжаем", error=str(e))
        return True

# -------------------- Playwright --------------------
async def launch():
    pw = await async_playwright().start()
    launch_kwargs = {
        "headless": HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--window-size=1280,900",
        ],
    }
    browser = await pw.chromium.launch(**launch_kwargs)

    context_kwargs: Dict[str, Any] = dict(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1280, "height": 900},
        storage_state=STORAGE_STATE if Path(STORAGE_STATE).exists() else None,
    )

    proxy = _playwright_proxy()
    if proxy:
        context_kwargs["proxy"] = proxy

    context = await browser.new_context(**context_kwargs)
    context.set_default_navigation_timeout(60_000)
    context.set_default_timeout(30_000)
    await context.set_extra_http_headers({"Accept-Language": "ru-RU,ru;q=0.9"})

    if BLOCK_MEDIA:
        async def route_handler(route):
            rt = route.request.resource_type
            if rt in {"media", "font"}:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_handler)

    return pw, browser, context

async def smart_goto(page, url: str) -> bool:
    """Более устойчивое открытие страницы категории/карточки."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        return True
    except Exception:
        pass
    # Fallback: быстрый commit → ждём DOM и якоря
    try:
        await page.goto(url, wait_until="commit", timeout=30_000)
        await page.wait_for_load_state("domcontentloaded", timeout=20_000)
        await page.wait_for_selector("a", timeout=20_000)
        return True
    except Exception:
        return False

async def ensure_not_firewalled(page) -> bool:
    """Если капча/фаервол — ждём, что пользователь решит её (HEADLESS=0)."""
    html = await page.content()
    if not is_firewall(html):
        return True
    jlog("WARN", "Обнаружен firewall/captcha. Решите в окне браузера (если headful).")
    start = time.time()
    while time.time() - start < 180:  # 3 минуты
        try:
            html = await page.content()
            if not is_firewall(html):
                return True
        except Exception:
            pass
        await asyncio.sleep(2)
    save_snapshot(html, f"captcha_{int(time.time())}")
    return False

# -------------------- Сбор ссылок из категории --------------------
async def collect_links(page, max_links: int, start_url: str) -> List[str]:
    result, seen = [], set()

    async def harvest() -> Tuple[int, int]:
        hrefs = await page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href') || '')")
        total, matched = 0, 0
        for h in hrefs:
            if not h:
                continue
            total += 1
            h = normalize_url(h)
            if is_item_url(h) and h not in seen:
                matched += 1
                seen.add(h)
                result.append(h)
        return total, matched

    total, matched = await harvest()
    jlog("INFO", "Диагностика ссылок (первый проход)", total_anchors=total, matched=matched)

    stagnant, last_len = 0, len(result)
    while len(result) < max_links and stagnant < 8:
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        await _nap()
        total, matched = await harvest()
        jlog("INFO", "Диагностика ссылок (скролл)", total_anchors=total, matched=matched, collected=len(result))
        if len(result) == last_len:
            stagnant += 1
        else:
            stagnant, last_len = 0, len(result)

    if not result:
        html = await page.content()
        save_snapshot(html, "category_zero_links")

    return result[:max_links]

# -------------------- Рейтинг и отзывы --------------------
NUM_RE = re.compile(r"([0-5](?:[.,]\d)?)\s*(?:из\s*5)?", re.I)

def _num_0_5(s: str) -> Optional[float]:
    if not s:
        return None
    m = NUM_RE.search(s)
    if not m:
        return None
    try:
        return max(0.0, min(5.0, float(m.group(1).replace(",", "."))))
    except Exception:
        return None

def _extract_seller_rating(soup: BeautifulSoup) -> float:
    # 1) Специфичный для Avito блок рейтинга на карточке
    cont = soup.select_one('.Ww4IN.seller-info-rating') or soup.select_one('[data-marker="sellerRate"], [data-marker*="sellerRate"]')
    if cont:
        # Часто цифра 5,0 лежит в span рядом со звёздами
        txt = cont.get_text(" ", strip=True)
        v = _num_0_5(txt)
        if v is not None:
            return v
        # и/или в aria-label звёзд
        lab = cont.select_one('[aria-label*="Рейтинг"], [aria-label*="из 5"], [title*="из 5"]')
        if lab:
            v = _num_0_5(lab.get("aria-label") or lab.get("title") or "")
            if v is not None:
                return v

    # 2) JSON-LD AggregateRating (редко, но вдруг)
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            txt = (node.get_text() or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if isinstance(obj, dict):
                    if obj.get("@type") == "AggregateRating":
                        v = _num_0_5(str(obj.get("ratingValue", "")))
                        if v is not None:
                            return v
                    ag = obj.get("aggregateRating")
                    if isinstance(ag, dict):
                        v = _num_0_5(str(ag.get("ratingValue", "")))
                        if v is not None:
                            return v
    except Exception:
        pass

    # 3) Любой элемент с aria/title/alt «из 5»
    for el in soup.select('[aria-label*="из 5"], [title*="из 5"]'):
        v = _num_0_5(el.get("aria-label") or el.get("title") or "")
        if v is not None:
            return v

    # 4) Фоллбек: по тексту всей страницы
    v = _num_0_5(soup.get_text(" ", strip=True))
    return float(v) if v is not None else 0.0

def _attrs_text_chain(node) -> str:
    parts = []
    for el in [node, *node.find_all(True, recursive=True)]:
        for a in ("aria-label", "title", "alt"):
            if el.has_attr(a) and el.get(a):
                parts.append(str(el.get(a)))
    return " | ".join(parts)

EXCLUDE_SNIPPETS = ("Вы открыли объявление в Бизнес 360",)

def _parse_reviews_from_html(html: str, limit: int = 50) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    out: List[Dict[str, Any]] = []
    seen = set()

    # Берём живые контейнеры + явные review-маркеры / звёзды
    containers = soup.select('[data-marker*="review"], [data-marker*="feedback"], article, section, li, div')
    for c in containers:
        txt = c.get_text(" ", strip=True)
        if not txt or len(txt) < 40:
            continue
        if any(s in txt for s in EXCLUDE_SNIPPETS):
            continue
        # фильтр на «похожесть» на отзыв
        if not ("Сделка состоялась" in txt or re.search(r"\b(Покупатель|Продавец)\b", txt, re.I) or re.search(r"\bиз\s*5\b", txt)):
            continue

        # Оценка: сначала по атрибутам (aria/title/alt)
        rating = _num_0_5(_attrs_text_chain(c))
        # затем по тексту
        if rating is None:
            rating = _num_0_5(txt)
        # затем по вложенным звёздам Avito (data-marker="review(...)/score/star-x")
        if rating is None:
            star_nodes = c.select('[data-marker*="/score/star-"], [class*="star"]')
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
        out.append({"review": txt, "rate": float(rating or 0.0)})
        if len(out) >= limit:
            break

    return out

async def _open_reviews_ui(page) -> str:
    """
    Пытаемся открыть «Отзывы»:
      - модалка (возврат 'dialog')
      - переход на страницу профиля (возврат 'page')
      - иначе ''.
    """
    # 1) Ссылка/кнопка «Отзывы» / «N отзывов»
    try:
        loc = page.locator('a:has-text("отзыв"), button:has-text("отзыв"), [data-marker="rating-caption/rating"]', has_text=re.compile("отзыв", re.I))
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
                # возможно, открылся профиль
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

async def _collect_reviews(page, mode: str, limit: int = 50) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
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

        batch = _parse_reviews_from_html(html, limit=limit - len(out))
        before = len(out)
        for b in batch:
            if b not in out:
                out.append(b)
                if len(out) >= limit:
                    break

        if len(out) >= limit:
            break

        stagnant = stagnant + 1 if len(out) == before else 0
        if stagnant >= 3:
            break

        try:
            if mode == "dialog":
                await page.evaluate("""
                    () => { const d = document.querySelector('[role="dialog"]');
                            if (d) d.scrollTop = d.scrollHeight; }
                """)
            else:
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            break
        await _nap()
    return out

# -------------------- Парсинг карточки --------------------
async def parse_item(page_url: str, context) -> Optional[Dict[str, Any]]:
    import html as htmllib

    url = normalize_url(page_url)
    page = await context.new_page()
    ok = await smart_goto(page, url)
    if not ok:
        jlog("WARN", "Таймаут навигации к карточке", url=url)
        await page.close(); return None

    # ждём что-то осмысленное (title/meta) ИЛИ firewall
    try:
        await page.wait_for_selector(
            'meta[property="og:title"], [data-marker="item-view/title"], h1, '
            'h2.firewall-title, #geetest_captcha, #h-captcha',
            timeout=15_000
        )
    except Exception:
        jlog("WARN", "Селектор карточки не дождался — парсим как есть", url=url)

    await page.wait_for_timeout(600)
    if not await ensure_not_firewalled(page):
        html_fw = await page.content()
        save_snapshot(html_fw, f"captcha_item_{int(time.time())}")
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
    price_num: float = 0.0
    mp = soup.find("meta", {"property": "product:price:amount"})
    if mp and mp.get("content"):
        price_num = price_to_number(mp["content"])
    if not price_num:
        pnode = soup.select_one('[itemprop="price"], [data-marker="item-view/price"], meta[itemprop="price"]')
        if pnode:
            text = pnode.get("content") or pnode.get_text(" ", strip=True)
            price_num = price_to_number(text)
    if not price_num:
        m = re.search(r"(\d[\d\s]{1,}\d)\s*₽", soup.get_text(" ", strip=True))
        if m:
            price_num = price_to_number(m.group(1))

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
    features_flat = "\n".join([re.sub(r"\s+", " ", t).strip() for t in features_texts if t.strip()])

    # seller rating
    seller_rate = _extract_seller_rating(soup)

    # отзывы (если удастся открыть)
    reviews: List[Dict[str, Any]] = []
    try:
        mode = await _open_reviews_ui(page)
        if mode:
            reviews = await _collect_reviews(page, mode=mode, limit=50)
    except Exception as e:
        jlog("WARN", "Отзывы: ошибка сбора", error=str(e), url=url)

    await page.close()
    return {
        "name": name or "",
        "url": canonical_url,
        "price": float(price_num or 0.0),
        "features": features_flat,
        "description": description or "",
        "seller_rate": float(seller_rate or 0.0),
        "seller_reviews": reviews or [],
    }

# -------------------- Главный поток --------------------
async def run(start_url: str, max_items: int, skip_robots: bool = True):
    """
    Открывает категорию/поиск, собирает до max_items карточек, парсит их
    и сохраняет по одной строке JSON в data/avito_items.jsonl (перезаписывает).
    """
    # robots — мягкая проверка (никогда не блокируем по 403)
    if skip_robots:
        await robots_soft_allow(start_url)

    # обнуляем результат
    OUTPUT_JSONL.write_text("", encoding="utf-8")
    jlog("INFO", "Файл результата обнулён", out=str(OUTPUT_JSONL))

    pw, browser, context = await launch()
    page = await context.new_page()
    ok = await smart_goto(page, start_url)
    if not ok:
        jlog("ERROR", "Категория не открылась (commit/DOM fallback)", url=start_url)
        await browser.close(); await pw.stop()
        return

    if not await ensure_not_firewalled(page):
        html = await page.content()
        save_snapshot(html, f"captcha_category_{int(time.time())}")
        jlog("ERROR", "Капча на категории не решена", url=start_url)
        await browser.close(); await pw.stop()
        return

    links = await collect_links(page, max_items, start_url)
    jlog("INFO", "Собрали ссылки", total=len(links), links=links)

    results = 0
    with open(OUTPUT_JSONL, "a", encoding="utf-8") as out:
        for href in links:
            tries, rec = 0, None
            while tries < MAX_RETRIES:
                tries += 1
                try:
                    rec = await parse_item(href, context)
                except Exception as e:
                    jlog("WARN", "Ошибка парсинга карточки", error=str(e), url=href)
                    rec = None
                if rec:
                    break
                backoff = min(6, 1.5 ** tries) + random.uniform(0, 0.6)
                jlog("WARN", "Повтор карточки", attempt=tries, sleep=round(backoff, 2), url=href)
                await asyncio.sleep(backoff)

            if rec:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                results += 1
            else:
                jlog("ERROR", "Не удалось извлечь карточку", url=href)

            await _nap()

    await browser.close(); await pw.stop()
    jlog("INFO", "Готово", extracted=results, out=str(OUTPUT_JSONL))


# Локальный тест:
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start-url", required=True)
    p.add_argument("--max-items", type=int, default=2)
    p.add_argument("--no-robots", action="store_true")
    args = p.parse_args()
    asyncio.run(run(args.start_url, args.max_items, skip_robots=not args.no_robots))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Avito category/search -> items scraper (Playwright + BS4)

Ключевые улучшения:
— Отзывы теперь берутся надёжно: модалка на карточке, отдельная страница отзывов, профиль продавца → вкладка «Отзывы».
— Сохраняем просмотренные объявления (data/seen_urls.json) и пропускаем их между запусками.
— Более устойчивые клики/скроллы, фоллбеки при «Execution context was destroyed».
— Мягкая проверка robots.txt (не блокирует), RU-локаль/таймзона, лёгкий stealth.
— JSONL перезаписывается на каждый запуск: data/avito_items.jsonl
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
from playwright.async_api import async_playwright
from playwright.async_api import Error as PWError

# -------------------- Папки/настройки --------------------
DATA_DIR = Path("./data")
SNAP_DIR = Path("./snapshots")
for d in (DATA_DIR, SNAP_DIR):
    d.mkdir(parents=True, exist_ok=True)

OUTPUT_JSONL = DATA_DIR / "avito_items.jsonl"
SEEN_FILE = DATA_DIR / "seen_urls.json"

HEADLESS = os.getenv("HEADLESS", "1").lower() in ("1", "true", "yes")
BLOCK_MEDIA = os.getenv("BLOCK_MEDIA", "0").lower() in ("1", "true", "yes")
STORAGE_STATE = os.getenv("STORAGE_STATE", str(DATA_DIR / "state.json"))
PROXY_URL = os.getenv("PROXY_URL", "").strip()
CHROME_CHANNEL = os.getenv("CHROME_CHANNEL", "").strip()  # обычно "chrome"

SLOW_MIN, SLOW_MAX = 0.6, 1.6
MAX_RETRIES = 3

EXEC_CTX_ERR_SNIPPETS = (
    "Execution context was destroyed",
    "Target closed",
    "Most likely because of a navigation",
)

# -------------------- Лог --------------------
def _now():
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()

def jlog(level: str, msg: str, **kw):
    print(json.dumps({"ts": _now(), "level": level.upper(), "msg": msg, **kw}, ensure_ascii=False))


# -------------------- Вспомогательное --------------------
def _nap(): return asyncio.sleep(random.uniform(SLOW_MIN, SLOW_MAX))

def _split_review_blocks(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """
    Возвращает список контейнеров, каждый из которых соответствует одному отзыву.
    Стратегия:
      - сначала блоки с [itemprop=review] или содержащие [itemprop=reviewBody]
      - затем элементы с маркерами review/feedback
      - затем article/li/div, где встречаются дата/роль/характерные слова
    """
    blocks = []

    # a) Явные schema.org
    for el in soup.select('[itemprop="review"]'):
        blocks.append(el)
    for body in soup.select('[itemprop="reviewBody"]'):
        # поднимаем до ближайшего article/li/div
        p = body
        while p and p.name not in ("article", "li", "div", "section"):
            p = p.parent
        if p and p not in blocks:
            blocks.append(p)

    # b) Маркеры интерфейса
    for el in soup.select('[data-marker*="review"], [data-marker*="feedback"]'):
        # ограничим до минимального осмысленного контейнера
        p = el
        depth = 0
        while p and depth < 4 and p.name not in ("article", "li", "div", "section"):
            p = p.parent; depth += 1
        if p and p not in blocks:
            blocks.append(p)

    # c) Эвристика: контейнеры с датами/ролями
    heur = []
    for el in soup.find_all(["article", "li", "div", "section"]):
        txt = el.get_text(" ", strip=True)
        if not txt or len(txt) < 30:
            continue
        has_date = bool(RE_DATE.search(txt))
        has_role = bool(re.search(r"\b(Покупатель|Продавец)\b", txt, re.I))
        if has_date or has_role or ("Сделка состоялась" in txt):
            heur.append(el)
    # Уберём крупные родительские дубликаты (оставим «самые нижние»)
    res = []
    for el in heur:
        if any(el is p or el in p.descendants for p in blocks):
            continue
        if any(el in q.descendants for q in heur if q is not el):
            continue
        res.append(el)

    # Итоговый список с приоритетом явных разметок
    return blocks + res

RU_MONTHS = (
    "января","февраля","марта","апреля","мая","июня",
    "июля","августа","сентября","октября","ноября","декабря"
)
RE_DATE = re.compile(rf"\b\d{{1,2}}\s+(?:{'|'.join(RU_MONTHS)})\s*(\d{{4}})?\b", re.I)

def _extract_seller_name(soup: BeautifulSoup) -> str:
    for sel in [
        '[data-marker="seller-info/name"]',
        'a[data-marker="seller-link/link"]',
        'a[href*="/profile/"]',
        'a[href*="/user/"]',
    ]:
        el = soup.select_one(sel)
        if el:
            nm = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
            if nm and len(nm) >= 2:
                return nm
    lab = soup.select_one('[aria-label*="Продавец"], [aria-label*="продавец"]')
    return re.sub(r"\s+", " ", lab.get_text(" ", strip=True)) if lab else ""


def _find_reviews_root(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    # В модалке/странице отзывов обычно есть заголовок «Отзывы» и контейнер списка
    candidates = []
    for h in soup.select('h1,h2,h3'):
        t = (h.get_text(" ", strip=True) or "").lower()
        if "отзыв" in t and "о модели" not in t:
            candidates.append(h)
    # если есть явные маркеры списка
    lists = soup.select(
        '[itemprop="review"], [itemprop="reviewBody"], '
        '[data-marker*="review"], [data-marker*="feedback"], ul, ol, section, article'
    )
    if candidates:
        # берём ближайший общий контейнер
        h = candidates[0]
        # поднимаемся чуть вверх, чтобы охватить список
        node = h
        for _ in range(3):
            if node and node.parent: node = node.parent
        return node or soup
    # fallback: если много reviewBody — берём их общий предок
    if lists:
        return soup
    return None

def _guess_reviews_urls(seller_link: str) -> List[str]:
    base = normalize_url(seller_link or "")
    if not base:
        return []
    urls = set()
    urls.add(base.rstrip("/") + "/reviews")
    urls.add(re.sub(r"/profile(/|$)", "/profile/", base).rstrip("/") + "/reviews")
    urls.add(re.sub(r"/user(s)?(/|$)", r"/user\1/", base).rstrip("/") + "/reviews")
    urls.add(base.rstrip("/") + "/otzyvy")
    return [normalize_url(u) for u in urls]

def _pick_review_text(block: BeautifulSoup, seller_name: str) -> str:
    """
    Достаём главный абзац/фразу из блока, отбрасывая:
      - имя/дату, «Покупатель/Продавец», «Сделка состоялась»
      - ответы продавца (по имени/шаблонам)
      - однословные/очень короткие/служебные фразы
    """
    # 1) приоритет schema.org
    body = block.select_one('[itemprop="reviewBody"]')
    if body:
        txt = re.sub(r"\s+", " ", body.get_text(" ", strip=True)).strip()
        txt = _strip_seller_reply_from_block(txt, seller_name)
        if len(txt) >= 20:
            return txt

    # 2) собрать кандидаты из p/div/span
    candidates: List[str] = []
    for el in block.find_all(["p", "div", "span"], recursive=True):
        t = re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()
        if not t or len(t) < 10:
            continue
        # вырезаем очевидные не-тексты
        low = t.lower()
        if any(b in low for b in ("покупатель", "продавец", "сделка состоялась")):
            continue
        # «Имя Фамилия 12 декабря 2024» — не тело
        if RE_DATE.search(t) and len(t.split()) <= 6:
            continue
        # короткие заголовки/лейблы
        if len(t) < 20 and not any(ch in t for ch in ".,!?:;") and len(t.split()) < 5:
            continue
        candidates.append(t)

    if not candidates:
        return ""

    # 3) вырежем из кандидатов «ответ продавца» и мусор
    clean: List[str] = []
    for t in candidates:
        t2 = _strip_seller_reply_from_block(t, seller_name)
        if not t2 or len(t2) < 20:
            continue
        if any(bad.lower() in t2.lower() for bad in EXCLUDE_SNIPPETS):
            continue
        clean.append(t2)

    if not clean:
        return ""

    # 4) Отдаём самый содержательный (макс. длина/слова)
    clean.sort(key=lambda s: (len(s.split()), len(s)), reverse=True)
    return clean[0]


def _looks_like_reviews_context(html: str, url: str = "") -> bool:
    h = (html or "").lower()
    u = (url or "").lower()
    if "/reviews" in u:
        return True
    # Есть явная разметка отзывов?
    if ('itemprop="reviewbody"' in h) or ('data-marker="review' in h) or ('data-marker="feedback' in h):
        return True
    # Заголовок/сигнальные слова
    if "отзывы" in h and "о модели" not in h:
        return True
    # Много дат → похоже на ленту отзывов
    months = ("января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря")
    if sum(h.count(m) for m in months) >= 3:
        return True
    return False

def _is_seller_reply(txt: str, seller_name: str) -> bool:
    if not txt: return False
    t = txt.strip()
    if "ответ продавца" in t.lower() or t.lower().startswith("продавец"):
        return True
    if seller_name:
        rx = rf"^{re.escape(seller_name).replace(' ','\\s+')}\\s+\\d{{1,2}}\\s+({'|'.join(RU_MONTHS)})\\b"
        if re.search(rx, t, re.I):  # «<Имя> 12 июня …»
            return True
    if len(t) <= 220 and any(x in t for x in ("Спасибо за", "Благодарим", "Будем рады", "Спасибо большое")):
        return True
    return False

def _strip_seller_reply_from_block(txt: str, seller_name: str) -> str:
    if not txt: return ""
    t = re.sub(r"\s+", " ", txt).strip()
    if not seller_name:
        return re.split(r"(?:Ответ продавца|Продавец)\b", t, 1)[0].strip()
    rx = rf"(?:Ответ продавца|Продавец|{re.escape(seller_name)})\s+\d{{1,2}}\s+({'|'.join(RU_MONTHS)})\b"
    parts = re.split(rx, t, 1, flags=re.I)
    return (parts[0] if parts else t).strip()

def normalize_url(u: str) -> str:
    if not u:
        return u
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin("https://www.avito.ru", u)
    sp = urlsplit(u)
    u = urlunsplit((sp.scheme, sp.netloc, sp.path, "", ""))  # чистим query/fragment
    # режем UTM и лишние параметры, которые плодят дубликаты
    u = re.sub(r"([?&])utm_[^=&]+=[^&]+", r"\1", u)
    u = re.sub(r"([?&])s=[^&]+", r"\1", u)
    return u.rstrip("?&").rstrip("#")

def is_item_url(u: str) -> bool:
    try:
        pu = urlparse(u)
        if pu.netloc not in {"avito.ru", "www.avito.ru", "m.avito.ru"}:
            return False
        parts = (pu.path or "/").strip("/").split("/")
        # /<city>/<category>/<slug_id>
        return len(parts) == 3 and bool(re.fullmatch(r".+_\d{7,}", parts[-1]))
    except Exception:
        return False

def price_to_number(text: str) -> float:
    if not text:
        return 0.0
    d = re.sub(r"[^\d]", "", text)
    return float(d) if d else 0.0

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

def load_seen() -> set:
    if SEEN_FILE.exists():
        try:
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return set()

def save_seen(seen: set) -> None:
    try:
        SEEN_FILE.write_text(json.dumps(sorted(seen), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# -------------------- Прокси для Playwright/httpx --------------------
def _playwright_proxy() -> Optional[Dict[str, str]]:
    if not PROXY_URL:
        return None
    try:
        pu = urlparse(PROXY_URL)
        server = f"{pu.scheme}://{pu.hostname}:{pu.port}"
        out: Dict[str, str] = {"server": server}
        if pu.username: out["username"] = pu.username
        if pu.password: out["password"] = pu.password
        return out
    except Exception:
        return {"server": PROXY_URL}

def _httpx_client() -> httpx.AsyncClient:
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ru-RU,ru;q=0.9"}
    try:
        proxies = {"http://": PROXY_URL, "https://": PROXY_URL} if PROXY_URL else None
        return httpx.AsyncClient(headers=headers, timeout=20.0,
                                 follow_redirects=True, verify=True, proxies=proxies)
    except TypeError:
        return httpx.AsyncClient(headers=headers, timeout=20.0,
                                 follow_redirects=True, verify=True)


# -------------------- Robots (мягко) --------------------
async def robots_soft_allow(start_url: str) -> bool:
    try:
        pu = urlparse(start_url)
        robots_url = f"{pu.scheme}://{pu.netloc}/robots.txt"
        async with _httpx_client() as client:
            r = await client.get(robots_url)
        if r.status_code != 200:
            jlog("WARN", "robots.txt недоступен — продолжаем", status=r.status_code, robots_url=robots_url)
            return True
        # Грубая проверка, результат не блокирует запуск
        txt = r.text
        path = "/" + "/".join(start_url.split("/", 3)[3:]).split("?")[0]
        dis, allow, ua_any = [], [], False
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"): continue
            if line.lower().startswith("user-agent"):
                ua_any = (line.split(":",1)[1].strip() == "*")
            elif ua_any and line.lower().startswith("disallow"):
                dis.append(line.split(":",1)[1].strip())
            elif ua_any and line.lower().startswith("allow"):
                allow.append(line.split(":",1)[1].strip())
        deny = [d for d in dis if d and path.startswith(d)]
        if deny:
            jlog("WARN", "robots.txt: путь в Disallow — продолжаем на свой риск", path=path)
        return True
    except Exception as e:
        jlog("WARN", "robots.txt: ошибка чтения — продолжаем", error=str(e))
        return True


# -------------------- Playwright --------------------
async def launch():
    pw = await async_playwright().start()

    launch_kwargs: Dict[str, Any] = {
        "headless": HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--window-size=1280,900",
            "--lang=ru-RU",
        ],
    }
    proxy = _playwright_proxy()
    if proxy:
        launch_kwargs["proxy"] = proxy

    if CHROME_CHANNEL:
        browser = await pw.chromium.launch(channel=CHROME_CHANNEL, **launch_kwargs)
    else:
        browser = await pw.chromium.launch(**launch_kwargs)

    context = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        viewport={"width": 1280, "height": 900},
        storage_state=STORAGE_STATE if Path(STORAGE_STATE).exists() else None,
    )
    await context.set_extra_http_headers({
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://www.google.com/",
    })
    # Геолокация Россия
    try:
        await context.grant_permissions(["geolocation"], origin="https://www.avito.ru")
        await context.set_geolocation({"latitude": 55.751244, "longitude": 37.618423})
    except Exception:
        pass

    # Лёгкий stealth
    await context.add_init_script("""
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU','ru','en-US','en']});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
try {
  Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
  Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
} catch(e) {}
""")

    context.set_default_navigation_timeout(60_000)
    context.set_default_timeout(30_000)
    await context.set_extra_http_headers({"Accept-Language": "ru-RU,ru;q=0.9"})

    if BLOCK_MEDIA:
        async def route_handler(route):
            if route.request.resource_type in {"media"}:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_handler)

    return pw, browser, context


async def smart_goto(page, url: str) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        return True
    except Exception:
        pass
    try:
        await page.goto(url, wait_until="commit", timeout=30_000)
        await page.wait_for_load_state("domcontentloaded", timeout=20_000)
        await page.wait_for_selector("a", timeout=20_000)
        return True
    except Exception:
        return False


async def ensure_not_firewalled(page) -> bool:
    html = await page.content()
    if not is_firewall(html):
        return True
    if HEADLESS:
        return False

    jlog("WARN", "Обнаружен firewall/captcha. Решите в окне браузера.")
    start = time.time()
    while time.time() - start < 180:
        try:
            html = await page.content()
            if not is_firewall(html):
                return True
        except Exception:
            pass
        await asyncio.sleep(2)
    save_snapshot(html, f"captcha_{int(time.time())}")
    return False


# -------------------- Сбор ссылок категории --------------------
async def collect_links(page, max_links: int, start_url: str, seen: set) -> List[str]:
    result, seen_local = [], set()

    async def harvest() -> Tuple[int, int]:
        try:
            hrefs = await page.eval_on_selector_all(
                "a",
                "els => els.map(e => (e.href || e.getAttribute('href') || ''))"
            )
        except Exception:
            hrefs = await page.evaluate(
                "Array.from(document.querySelectorAll('a')).map(a => (a.href || a.getAttribute('href') || ''))"
            )
        total, matched = 0, 0
        for h in hrefs:
            if not h:
                continue
            total += 1
            h = normalize_url(h)
            if is_item_url(h) and h not in seen_local and h not in seen:
                matched += 1
                seen_local.add(h)
                result.append(h)
        return total, matched

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except Exception:
        pass

    total, matched = await harvest()
    jlog("INFO", "Диагностика ссылок (первый проход)", total_anchors=total, matched=matched)

    stagnant, last_len = 0, len(result)
    while len(result) < max_links and stagnant < 8:
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except Exception:
            pass
        await _nap()
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5_000)
        except Exception:
            pass

        total, matched = await harvest()
        jlog("INFO", "Диагностика ссылок (скролл)", total_anchors=total, matched=matched, collected=len(result))
        if len(result) == last_len:
            stagnant += 1
        else:
            stagnant, last_len = 0, len(result)

    if not result:
        save_snapshot(await page.content(), "category_zero_links")

    return result[:max_links]


# -------------------- Рейтинг и отзывы --------------------
NUM_RE = re.compile(r"([0-5](?:[.,]\d)?)\s*(?:из\s*5)?", re.I)
def _num_0_5(s: str) -> Optional[float]:
    if not s: return None
    m = NUM_RE.search(s)
    if not m: return None
    try:
        return max(0.0, min(5.0, float(m.group(1).replace(",", "."))))
    except Exception:
        return None

def _extract_review_body_from_block(text: str, seller_name: str) -> str:
    """
    Берём «хвост» после «Сделка состоялась …»; вычищаем товары/служебные куски; режем ответы продавца.
    """
    if not text:
        return ""
    t = re.sub(r"\s+", " ", text).strip()
    # Отрезаем всё после возможного «ответа продавца»
    t = _strip_seller_reply_from_block(t, seller_name)

    # ищем «Сделка состоялась»
    if "Сделка состоялась" in t:
        tail = t.split("Сделка состоялась", 1)[1]
    else:
        # fallback: если есть роль/дата — работаем с исходником
        tail = t

    # убираем ведущие маркеры/буллеты
    tail = re.sub(r"^[\s•·,;:|-]+", " ", tail).strip()

    # режем по буллетам/«·» и крупным разделителям
    parts = [p.strip(" .·•\u2022") for p in re.split(r"(?:[•·]\s*|\s{2,}| — )", tail) if p.strip()]
    # отбрасываем короткие/служебные и товарные названия
    candidates = []
    for p in parts:
        if any(bad.lower() in p.lower() for bad in EXCLUDE_SNIPPETS):
            continue
        if re.search(r"\b(Покупатель|Продавец)\b", p, re.I):
            continue
        # короткие товарные названия часто < 60 символов
        if re.search(r"\b(контейнер|iphone|телефон|морской|фут|gb|гб)\b", p, re.I) and len(p) < 60:
            continue
        if len(p) >= 20:
            candidates.append(p)

    if not candidates:
        body = tail.strip()
    else:
        # берём самый содержательный фрагмент
        body = max(candidates, key=len).strip()

    body = _strip_seller_reply_from_block(body, seller_name)
    return body.strip()

def _extract_seller_rating(soup: BeautifulSoup) -> float:
    try:
        container = None
        rate_widget = soup.select_one('[data-marker="sellerRate"]')
        if rate_widget:
            container = rate_widget.find_parent(True)
        if not container:
            container = soup.select_one('.seller-info-rating')
        if container:
            for sp in container.find_all('span', recursive=True):
                txt = (sp.get_text(strip=True) or '').replace(',', '.')
                if re.fullmatch(r'[0-5](?:\.\d)?', txt):
                    try:
                        v = float(txt)
                        if 0.0 <= v <= 5.0:
                            return v
                    except Exception:
                        pass
            v = _num_0_5(container.get_text(" ", strip=True))
            if v is not None:
                return v
    except Exception:
        pass

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

    for el in soup.select('[aria-label], [title], [alt]'):
        s = (el.get("aria-label") or el.get("title") or el.get("alt") or "").strip()
        if not s:
            continue
        if ("из 5" in s) or ("рейтинг" in s.lower()) or ("оцен" in s.lower()):
            v = _num_0_5(s)
            if v is not None:
                if "из 5" not in s and re.fullmatch(r"Рейтинг\s*[1-5]\b", s):
                    continue
                return v

    v = _num_0_5(soup.get_text(" ", strip=True))
    return float(v) if v is not None else 0.0


def _attrs_text_chain(node) -> str:
    parts = []
    for el in [node, *node.find_all(True, recursive=True)]:
        for a in ("aria-label", "title", "alt"):
            if el.has_attr(a) and el.get(a):
                parts.append(str(el.get(a)))
    return " | ".join(parts)

EXCLUDE_SNIPPETS = (
    # общий мусор/шапки
    "Вы открыли объявление в Бизнес 360",
    "Рейтинг — это среднее арифметическое оценок пользователей",
    "на основании", "Написать отзыв", "Оставить отзыв",
    "Пожаловаться", "Подробнее", "Показать ещё", "Показать больше",
    # профильный мусор
    "Активные", "Завершённые", "Поиск в профиле", "Ещё", "О компании", "Адрес", "Оплата",
    # не брать «отзывы о модели»
    "Отзывы о модели", "отзывов о модели",
)

def _parse_reviews_from_html(html: str, limit: int = 50, seller_name: str = "") -> List[Dict[str, str]]:
    """
    Стабильный парсинг ТОЛЬКО текстов отзывов покупателей.
    """
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    if not _looks_like_reviews_context(html):
        return []

    # 0) JSON-LD reviewBody — быстрый сбор (если есть)
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            payload = (node.get_text() or "").strip()
            if not payload:
                continue
            data = json.loads(payload)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if not isinstance(obj, dict):
                    continue
                # одиночный review
                if obj.get("@type") in ("Review", "UserReview"):
                    rb = obj.get("reviewBody") or obj.get("description")
                    if rb:
                        t = _strip_seller_reply_from_block(str(rb), seller_name)
                        t = re.sub(r"\s+", " ", t).strip()
                        if t and len(t) >= 20:
                            low = t.lower()
                            if not any(b in low for b in ( "отзывы о модели", "на основании" )):
                                nt = t.lower()
                                if nt not in seen:
                                    seen.add(nt); out.append({"review": t})
                # коллекция review[]
                if "review" in obj:
                    revs = obj["review"]
                    arr = revs if isinstance(revs, list) else [revs]
                    for r in arr:
                        if isinstance(r, dict):
                            rb = r.get("reviewBody") or r.get("description")
                            if rb:
                                t = _strip_seller_reply_from_block(str(rb), seller_name)
                                t = re.sub(r"\s+", " ", t).strip()
                                if t and len(t) >= 20:
                                    nt = t.lower()
                                    if nt not in seen:
                                        seen.add(nt); out.append({"review": t})
                        if len(out) >= limit:
                            return out[:limit]
    except Exception:
        pass  # JSON-LD может быть «грязным»

    if len(out) >= limit:
        return out[:limit]

    # 1) DOM-блоки отзывов
    blocks = _split_review_blocks(soup)
    for b in blocks:
        t = _pick_review_text(b, seller_name)
        if not t:
            continue
        nt = t.lower()
        if nt in seen:
            continue
        seen.add(nt)
        out.append({"review": t})
        if len(out) >= limit:
            break

    return out[:limit]


async def _open_reviews_ui(page):
    """
    Попасть в отзывы: 1) диалог (role=dialog), 2) клик «Отзывы/Рейтинг» с перехватом popup вкладки,
    3) профиль продавца → вкладка «Отзывы» или прямые /reviews.
    Возвращает (mode, target_page). Используем Playwright `context.expect_page` для новых вкладок,
    ARIA role=dialog для модалки. :contentReference[oaicite:5]{index=5}
    """
    import re as _re
    await page.wait_for_timeout(700)

    try:
        if await page.locator('[role="dialog"]').count():
            return "dialog", page
    except Exception:
        pass

    try:
        variants = [
            'a[data-marker="rating-caption/rating"]',
            '[data-marker*="rating"] a',
            '[data-marker*="seller"] a:has-text("Отзывы")',
            'a:has-text("Отзывы")', 'button:has-text("Отзывы")',
            'a:has-text("Рейтинг")', 'button:has-text("Рейтинг")',
        ]
        for sel in variants:
            loc = page.locator(sel).first
            if not await loc.count():
                continue
            try:
                await loc.scroll_into_view_if_needed(timeout=3000)
            except Exception:
                pass
            new_page = None
            try:
                async with page.context.expect_page() as ep:
                    await loc.click(timeout=4000, force=True)
                new_page = await ep.value    # новая вкладка/окно с отзывами
            except Exception:
                try:
                    await loc.click(timeout=4000, force=True)
                except Exception:
                    try:
                        el = await loc.element_handle()
                        if el: await page.evaluate('(e)=>e.click()', el)
                    except Exception:
                        pass

            await page.wait_for_timeout(600)
            if await page.locator('[role="dialog"]').count():
                return "dialog", page

            target = new_page or page
            try:
                await target.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass
            # проверяем характерные признаки страницы отзывов
            if await target.locator('h1:has-text("Отзывы"), h2:has-text("Отзывы"), :has-text("Сделка состоялась")').count():
                return "page", target
    except Exception:
        pass

    # Профиль продавца → вкладка/URL «Отзывы»
    try:
        seller_link = None
        for sel in [
            'a[data-marker="seller-link/link"]',
            'a[href*="/profile/"]',
            'a[href*="/user/"]',
            'a:has([data-marker="seller-info/name"])',
        ]:
            cand = page.locator(sel).first
            if await cand.count():
                href = await cand.get_attribute("href")
                if href:
                    seller_link = normalize_url(href); break

        if seller_link:
            prof = await page.context.new_page()
            if await smart_goto(prof, seller_link):
                try:
                    await prof.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass

                tab = prof.locator('a,button').filter(has_text=_re.compile(r'Отзывы', _re.I))
                if await tab.count():
                    try:
                        await tab.first.click(timeout=5000, force=True)
                        await prof.wait_for_load_state("domcontentloaded", timeout=8000)
                    except Exception:
                        pass
                else:
                    for u in _guess_reviews_urls(seller_link):
                        if await smart_goto(prof, u):
                            try:
                                await prof.wait_for_load_state("domcontentloaded", timeout=8000)
                            except Exception:
                                pass
                            if await prof.locator(':has-text("Сделка состоялась"), [itemprop="reviewBody"]').count():
                                return "page", prof
                return "page", prof
    except Exception:
        pass

    return "", None



async def _collect_reviews(target_page, mode: str, limit: int = 50, seller_name: str = "") -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    try:
        if mode == "dialog":
            await target_page.wait_for_selector('[role="dialog"]', timeout=5000)
        else:
            await target_page.wait_for_selector(
                '[itemprop="reviewBody"], [itemprop="review"], [data-marker*="review"], [data-marker*="feedback"], h1:has-text("Отзывы"), h2:has-text("Отзывы")',
                timeout=8000
            )
    except Exception:
        pass

    stagnant = 0
    for _ in range(30):
        if mode == "dialog":
            html = await target_page.evaluate("""
                () => {
                  const d = document.querySelector('[role="dialog"]');
                  return d ? d.innerHTML : document.documentElement.innerHTML;
                }
            """)
        else:
            html = await target_page.content()

        if not _looks_like_reviews_context(html, getattr(target_page, "url", "")):
            batch = []
        else:
            batch = _parse_reviews_from_html(html, limit=limit - len(out), seller_name=seller_name)

        before = len(out)
        for b in batch:
            if b not in out:
                out.append(b)
                if len(out) >= limit:
                    break
        if len(out) >= limit:
            break

        stagnant = stagnant + 1 if len(out) == before else 0
        if stagnant >= 8:
            break

        try:
            # «Показать ещё»/«Ещё»/«Показать больше»
            clicked_more = False
            more_sel = (
                'button:has-text("Показать ещё"), button:has-text("Показать еще"), '
                'button:has-text("Ещё"), button:has-text("Еще"), button:has-text("Показать больше")'
            )
            btn = target_page.locator(more_sel).first
            if await btn.count():
                try:
                    await btn.click(timeout=3000, force=True)
                    clicked_more = True
                except Exception:
                    pass

            if not clicked_more and mode == "dialog":
                await target_page.evaluate("""() => { const d = document.querySelector('[role="dialog"]'); if (d) d.scrollTop = d.scrollHeight; }""")
            else:
                # Прокрутить к последнему видимому отзыву/телу
                scrolled = False
                for sel in ['[itemprop="reviewBody"]', '[itemprop="review"]', '[data-marker*="review"]', '[data-marker*="feedback"]', 'main', 'body']:
                    try:
                        await target_page.locator(sel).last.scroll_into_view_if_needed(timeout=1200)
                        scrolled = True
                        break
                    except Exception:
                        continue
                if not scrolled:
                    try:
                        await target_page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                    except Exception:
                        pass
                try:
                    await target_page.mouse.wheel(0, 1800)
                except Exception:
                    pass
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
        save_snapshot(await page.content(), f"captcha_item_{int(time.time())}")
        await page.close(); return None

    html = await page.content()
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

    price_num = 0.0
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

    description = ""
    try:
        for node in soup.select('script[type="application/ld+json"]'):
            txt = (node.get_text() or "").strip()
            if not txt: continue
            data = json.loads(txt)
            datas = data if isinstance(data, list) else [data]
            for obj in datas:
                if isinstance(obj, dict) and obj.get("@type") == "Product" and obj.get("description"):
                    description = obj["description"]; break
            if description: break
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
                if line: features_texts.append(line)
        else:
            txt = cont.get_text("\n", strip=True)
            if txt: features_texts.append(txt)
    features_flat = "\n".join([re.sub(r"\s+", " ", t).strip() for t in features_texts if t.strip()])

    seller_rate = _extract_seller_rating(soup)
    seller_name = _extract_seller_name(soup)
    # ---- Отзывы ----
    reviews: List[Dict[str, str]] = []
    ext_page_to_close = None
    try:
        await page.wait_for_timeout(600)

        mode, target = await _open_reviews_ui(page)
        if mode == "dialog" and target:
            reviews = await _collect_reviews(target, mode="dialog", limit=50, seller_name=seller_name)
        elif mode == "page" and target:
            if target is not page:
                ext_page_to_close = target
            reviews = await _collect_reviews(target, mode="page", limit=50, seller_name=seller_name)
        else:
            jlog("INFO", "Отзывы: вход не найден (это ок для части объявлений)", url=url)
    except Exception as e:
        jlog("WARN", "Отзывы: ошибка сбора", error=str(e), url=url)
    finally:
        if ext_page_to_close is not None:
            try:
                await ext_page_to_close.close()
            except Exception:
                pass

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
    if skip_robots:
        await robots_soft_allow(start_url)

    # загружаем уже обработанные
    seen = load_seen()

    # очищаем результирующий файл под текущий запуск
    OUTPUT_JSONL.write_text("", encoding="utf-8")
    jlog("INFO", "Файл результата обнулён", out=str(OUTPUT_JSONL), seen_count=len(seen))

    pw, browser, context = await launch()
    page = await context.new_page()
    warm = await context.new_page()
    await smart_goto(warm, "https://www.avito.ru/")
    await warm.wait_for_timeout(1200)
    await warm.close()
    ok = await smart_goto(page, start_url)
    if not ok:
        jlog("ERROR", "Категория не открылась (commit/DOM fallback)", url=start_url)
        await browser.close(); await pw.stop(); return

    if not await ensure_not_firewalled(page):
        save_snapshot(await page.content(), f"captcha_category_{int(time.time())}")
        jlog("ERROR", "Капча на категории. Нужен новый state/прокси", url=start_url)
        await browser.close(); await pw.stop(); return
    else:
        try:
            await context.storage_state(path=STORAGE_STATE)
        except Exception:
            pass

    links = await collect_links(page, max_items, start_url, seen)
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
                    jlog("WARN", "Ошибка парсинга карточки", error=str(e), url=href); rec = None
                if rec:
                    break
                backoff = min(6, 1.5 ** tries) + random.uniform(0, 0.6)
                jlog("WARN", "Повтор карточки", attempt=tries, sleep=round(backoff, 2), url=href)
                await asyncio.sleep(backoff)

            if rec:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                results += 1
                # добавляем в seen канонический URL записи
                try:
                    seen.add(rec.get("url") or href)
                except Exception:
                    seen.add(href)
                save_seen(seen)
            else:
                jlog("ERROR", "Не удалось извлечь карточку", url=href)
            await _nap()

    await browser.close(); await pw.stop()
    jlog("INFO", "Готово", extracted=results, out=str(OUTPUT_JSONL), seen_total=len(seen))


# -------------------- CLI --------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--start-url", required=True)
    p.add_argument("--max-items", type=int, default=2)
    p.add_argument("--no-robots", action="store_true")
    args = p.parse_args()
    asyncio.run(run(args.start_url, args.max_items, skip_robots=not args.no_robots))

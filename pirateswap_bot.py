#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PirateSwap closed Telegram bot (мониторинг + рассылка разделены).

Архитектура (3 потока + общий замок):
  1. HTTP-сервер (главный поток) — /health -> 200 для UptimeRobot, чтобы Render
     Free не засыпал. / -> краткий статус.
  2. Поток-поллер сайта — крутится ВСЕГДА, независимо от пользователей. При дропе
     рассылает алерт всем из белого списка, у кого notify=true.
  3. Поток Telegram getUpdates (long-polling) — /start, /status, inline-кнопки.
     Переключает флаг notify ТОЛЬКО нажавшего, на мониторинг не влияет.

Закрытый бот: фиксированный белый список chat_id. Любой чужой chat_id — игнор.

Хранение: notify-флаги в JSON (эфемерный на Render — после рестарта все
notify=true). Лог событий — JSON-lines. Без SQLite.

Зависимости: только стандартная библиотека Python 3.8+.

Запуск:
    PIRATE_TG_TOKEN=... python pirateswap_bot.py            # бот целиком (как на Render)
    PIRATE_TG_TOKEN=... python pirateswap_bot.py --once     # один проход опроса, без бота
    python pirateswap_bot.py --selftest                     # проверка логики на фейковых данных
"""

import os
import sys
import json
import time
import random
import threading
import datetime as dt
import urllib.parse
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ============================ КОНФИГ ============================

# Предметы: имя -> его marketHashNameHashCode (РОВНО ОДИН код на предмет;
# несколько кодов через запятую дают HTTP 500).
TARGETS = {
    "Sealed Dead Hand Terminal": -1818601513,
    "Fracture Case":             -87706406,
    "Kilowatt Case":             1757559884,
    "Revolution Case":           1299851513,
    "Recoil Case":               29464456,
    # Контрольный: всегда в наличии. По нему НЕ шлём дроп-алерты — только
    # индикатор "бот видит сток" в /status и heartbeat.
    "Prisma Case":               -2067952841,
}

CONTROL_ITEMS = {"Prisma Case"}

# Закрытый бот: разрешённые chat_id (и только они).
WHITELIST = {682446275, 770511678, 778333031}
# Кому слать heartbeat ("бот жив"): только админ.
ADMIN_CHAT = 770511678

# Эндпоинт ОБЯЗАТЕЛЬНО с v2 (без v2 фильтр игнорируется и возвращается мусор).
BASE_URL = "https://web.pirateswap.com/inventory/v2/ExchangerInventory"

PARAMS = {
    "orderBy": "price",
    "sortOrder": "DESC",
    "page": "1",
    "results": "40",
}

# --- Темп опроса (round-robin: ровный поток запросов без длинных простоев) ---
# Пауза между отдельными запросами. При 5 боевых предметах каждый
# перепроверяется ~раз в 5*avg(GAP) ≈ 25 с (раньше было ~70 с из-за паузы
# между обходами). Быстрее детект — больше шанс успеть купить.
ITEM_GAP_MIN = 4.0
ITEM_GAP_MAX = 6.0
# Контрольный Prisma опрашивается редко (он всегда в наличии, боевого смысла нет)
# — чтобы не тратить на него слоты round-robin. Раз в N секунд.
CONTROL_EVERY_SEC = 600

# Анти-ложная защита: сверяем marketNameHashCode каждого предмета с ожидаемым.
STRICT_HASH_CHECK = True

HEARTBEAT_HOURS = 6     # пинг "жив" админу раз в N часов (0 = выкл)

# Админ-алерт на серверные ошибки (5xx / Cloudflare 403/503). Чтобы при череде
# ошибок не словить спам — не чаще одного сообщения раз в N минут.
ERROR_ALERT_COOLDOWN_MIN = 15

# --- Telegram: токен ТОЛЬКО из окружения, без fallback в коде ---
TG_TOKEN = os.environ.get("PIRATE_TG_TOKEN", "").strip()

# Render передаёт порт через $PORT; локально — 10000.
HTTP_PORT = int(os.environ.get("PORT", "10000"))

STATE_FILE = os.environ.get("PIRATE_STATE_FILE", "notify_state.json")
LOG_FILE = os.environ.get("PIRATE_LOG_FILE", "events.jsonl")

# ---------------------- АВТОПОКУПКА ----------------------------
# Бот сам оформляет покупку в момент детекта (POST /Exchange/start/v2/{steamid}).
# ⚠️ Тратит реальные деньги и почти наверняка против ToS PirateSwap (риск бана).
#
# ДВОЙНОЙ ПРЕДОХРАНИТЕЛЬ:
#   1) AUTOBUY_ENABLED (env) — мастер-переключатель фичи. По умолчанию 0.
#   2) «Вооружение ценой» — даже при AUTOBUY_ENABLED=1 бот НИЧЕГО не покупает,
#      пока админ не задаст цену командой /setprice X в Telegram. Цена живёт
#      в памяти и СБРАСЫВАЕТСЯ при рестарте — после каждого перезапуска бот
#      снова «ждёт цену» и не покупает, пока ты её заново не выставишь.
# Kill switch: AUTOBUY_ENABLED=0 или /buyoff — покупки выкл (мониторинг живёт).
AUTOBUY_ENABLED = os.environ.get("AUTOBUY_ENABLED", "0") == "1"
AUTOBUY_DRY_RUN = os.environ.get("AUTOBUY_DRY_RUN", "1") != "0"     # по умолч. симуляция
# Какие предметы вообще разрешено автопокупать (по display-имени). Для теста —
# только Revolution Case. Можно переопределить env-ом (через запятую).
AUTOBUY_ITEMS = {s.strip() for s in
                 os.environ.get("AUTOBUY_ITEMS", "Revolution Case").split(",")
                 if s.strip()}
# Необязательный жёсткий потолок для /setprice (защита от опечатки). 0 = без потолка.
AUTOBUY_MAX_PRICE = float(os.environ.get("AUTOBUY_MAX_PRICE", "0") or 0)
AUTOBUY_DAILY_LIMIT = int(os.environ.get("AUTOBUY_DAILY_LIMIT", "20") or 20)  # макс покупок в сутки
AUTOBUY_PRICE_FIELD = os.environ.get("AUTOBUY_PRICE_FIELD", "price")   # какое поле цены слать: price | storePrice
PIRATE_BEARER = os.environ.get("PIRATE_BEARER", "").strip()           # Bearer-токен сайта (протухает!)
PIRATE_STEAMID = os.environ.get("PIRATE_STEAMID", "").strip()         # твой SteamID64 (публичный)

EXCHANGE_START_URL = "https://web.pirateswap.com/Exchange/start/v2/"
# Идентификатор сессии (как у браузера — один на вкладку). Один и тот же на
# авторизованном GET-инвентаре и на start/v2, чтобы резервация «сошлась».
SESSION_OPENED_AT = str(int(time.time() * 1000))

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://pirateswap.com",
    "Referer": "https://pirateswap.com/",
}

# ===============================================================
# Общее состояние между потоками

_state_lock = threading.Lock()       # защищает notify_state + запись STATE_FILE
_counts_lock = threading.Lock()      # защищает last_counts / last_poll_ts
_log_lock = threading.Lock()         # защищает запись LOG_FILE
_err_lock = threading.Lock()         # защищает _last_err_alert

_last_err_alert = None               # datetime последнего админ-алерта об ошибке

notify_state = {}                    # {"<chat_id>": {"notify": bool}}
last_counts = {mhn: None for mhn in TARGETS}   # последний известный count (None = ещё/ошибка)
last_market_names = {}               # mhn(display) -> реальный marketHashName из API (для ссылки)
last_poll_ts = None                  # datetime последнего успешного опроса


def now():
    return dt.datetime.now()


def ts():
    return now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{ts()}] {msg}", flush=True)


def log_event(kind, **fields):
    """Пишет строку события в JSON-lines лог (для анализа расписания)."""
    rec = {"ts": ts(), "kind": kind}
    rec.update(fields)
    line = json.dumps(rec, ensure_ascii=False)
    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        log(f"⚠️ не удалось записать лог-событие: {e}")


def item_link(name):
    return "https://pirateswap.com/exchanger?mhn=" + urllib.parse.quote(name)


# -------------------- notify-state (JSON) ----------------------

def state_load():
    """
    Загружает notify-флаги. Если файла нет / битый — создаёт из белого списка
    (все notify=true). Гарантирует, что в state есть ровно все из WHITELIST.
    """
    global notify_state
    data = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log(f"⚠️ STATE_FILE битый ({e}) — пересоздаю из белого списка.")
            data = {}

    fixed = {}
    for cid in WHITELIST:
        key = str(cid)
        prev = data.get(key) if isinstance(data, dict) else None
        notify = bool(prev.get("notify", True)) if isinstance(prev, dict) else True
        fixed[key] = {"notify": notify}

    with _state_lock:
        notify_state = fixed
        _state_save_locked()
    log("notify-state загружен: " + json.dumps(notify_state, ensure_ascii=False))


def _state_save_locked():
    """Сохранение под уже захваченным _state_lock."""
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(notify_state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log(f"⚠️ не удалось сохранить STATE_FILE: {e}")


def set_notify(chat_id, value):
    key = str(chat_id)
    with _state_lock:
        if key not in notify_state:           # не из белого списка — не трогаем
            return False
        notify_state[key]["notify"] = bool(value)
        _state_save_locked()
    return True


def get_notify(chat_id):
    with _state_lock:
        rec = notify_state.get(str(chat_id))
        return bool(rec["notify"]) if rec else False


def notify_recipients():
    """chat_id из белого списка, у кого notify=true."""
    with _state_lock:
        return [int(k) for k, v in notify_state.items() if v.get("notify")]


# -------------------------- Telegram ---------------------------

def tg_api(method, params, timeout=35):
    """Вызов Bot API. Возвращает dict result или None при ошибке."""
    if not TG_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    data = urllib.parse.urlencode(params).encode()
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            payload = json.loads(r.read().decode("utf-8", errors="replace"))
        if not payload.get("ok"):
            log(f"⚠️ TG {method} ok=false: {payload.get('description')}")
            return None
        return payload.get("result")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        log(f"⚠️ TG {method} HTTP {e.code}: {body}")
        return None
    except Exception as e:
        log(f"⚠️ TG {method} ошибка: {e}")
        return None


def tg_send(chat_id, text, reply_markup=None):
    params = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if reply_markup is not None:
        params["reply_markup"] = json.dumps(reply_markup)
    return tg_api("sendMessage", params) is not None


def tg_answer_callback(callback_id, text=""):
    tg_api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})


def tg_edit_text(chat_id, message_id, text, reply_markup=None):
    params = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if reply_markup is not None:
        params["reply_markup"] = json.dumps(reply_markup)
    return tg_api("editMessageText", params) is not None


def main_keyboard():
    return {"inline_keyboard": [[
        {"text": "🔕 Стоп", "callback_data": "n_off"},
        {"text": "🔔 Старт", "callback_data": "n_on"},
    ]]}


# Гасит старую reply-клавиатуру (от прежнего бота). Нельзя совмещать в одном
# сообщении с inline-кнопками — поэтому шлётся отдельным сообщением.
REMOVE_KEYBOARD = {"remove_keyboard": True}


def admin_error_alert(detail):
    """Шлёт админу ЛС о серверной ошибке, но не чаще ERROR_ALERT_COOLDOWN_MIN.
    Между алертами копит, сколько ошибок было подавлено, и сообщает это."""
    global _last_err_alert
    if not ADMIN_CHAT:
        return
    with _err_lock:
        n = now()
        if _last_err_alert is not None:
            mins = (n - _last_err_alert).total_seconds() / 60
            if mins < ERROR_ALERT_COOLDOWN_MIN:
                return                      # ещё в кулдауне — молчим
        _last_err_alert = n
    tg_send(ADMIN_CHAT, f"🛑 Серверная ошибка PirateSwap:\n{detail}\n"
                        f"(следующее такое уведомление — не раньше чем через "
                        f"{ERROR_ALERT_COOLDOWN_MIN} мин)")


def buy_keyboard(market_name):
    """Одна URL-кнопка «Купить» — тап открывает предмет на сайте (без чтения текста)."""
    return {"inline_keyboard": [[
        {"text": "🛒 Купить", "url": item_link(market_name)},
    ]]}


def broadcast_drop(text, reply_markup=None):
    """Шлёт алерт всем из белого списка, у кого notify=true."""
    recips = notify_recipients()
    sent = 0
    for cid in recips:
        if tg_send(cid, text, reply_markup=reply_markup):
            sent += 1
        time.sleep(0.2)   # бережём лимит Telegram
    log(f"📤 алерт разослан: {sent}/{len(recips)} получателей")
    return sent


# --------------------------- Запросы ---------------------------

def build_url(name, code):
    params = dict(PARAMS)
    params["searchPhrase"] = name
    params["marketHashNameHashCodes"] = str(int(code))
    return BASE_URL + "?" + urllib.parse.urlencode(params, safe=",")


def fetch(name, code, authed=False):
    """
    Возвращает (items_out:list[dict], foreign:int).
    items_out: [{"assetId","itemId","price","storePrice","tradableAfter","name"}].
    foreign — сколько отброшено как чужие (не тот hash code).
    authed=True — шлём с Bearer-токеном и x-session-opened-at: сервер привязывает
    показанные предметы к нашей сессии («резервация» перед start/v2).
    Бросает исключение при сетевой/HTTP ошибке.
    """
    url = build_url(name, code)
    headers = dict(HEADERS)
    if authed and PIRATE_BEARER:
        headers["Authorization"] = "Bearer " + PIRATE_BEARER
        headers["x-session-opened-at"] = SESSION_OPENED_AT
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    items = data.get("items") or []   # totalResults/totalPages баговые — не трогаем

    allowed = int(code)
    out = []
    foreign = 0
    for it in items:
        if STRICT_HASH_CHECK:
            ihash = it.get("marketNameHashCode")
            if ihash is not None and int(ihash) != allowed:
                foreign += 1
                continue
        aid = it.get("assetId")
        if aid is None:
            continue
        out.append({
            "assetId": str(aid),          # числовой id единицы — для дедупа/детекта
            "itemId": it.get("id"),       # GUID — нужен для покупки (start/v2)
            "price": it.get("price"),
            "storePrice": it.get("storePrice"),
            "tradableAfter": it.get("tradableAfter"),
            "name": it.get("marketHashName"),
        })
    return out, foreign


def format_price(p):
    if p is None:
        return "?"
    try:
        return f"{float(p):.2f}"
    except (TypeError, ValueError):
        return str(p)


def build_alert(display_name, items, new_items):
    """Короткий дроп-алерт: имя, count, цена, (tradableAfter если есть).
    Ссылка вынесена в кнопку «Купить» — чтобы покупать в один тап."""
    count = len(items)
    prices = [i["price"] for i in items if i.get("price") is not None]
    price_str = ""
    if prices:
        try:
            price_str = " · 💰 от " + format_price(min(prices, key=float))
        except (TypeError, ValueError):
            price_str = " · 💰 " + format_price(prices[0])

    # tradableAfter — только если есть в данных у новых предметов.
    ta = next((i["tradableAfter"] for i in new_items
               if i.get("tradableAfter")), None)
    ta_line = f"\n🔒 tradableAfter: {ta}" if ta else ""

    return (f"🔥 ДРОП! {display_name}\n"
            f"📦 {count} шт (новых: {len(new_items)}){price_str}{ta_line}")


# --------------------------- Автопокупка -----------------------

_buy_lock = threading.Lock()
_buy_day = None                  # дата, к которой относится счётчик
_buy_count = 0                   # сколько куплено за текущие сутки

_prices_lock = threading.Lock()
_item_prices = {}                # {display_name: max_price} — в памяти, сброс при рестарте

_pending_lock = threading.Lock()
_pending_item = None             # предмет, для которого админ сейчас вводит цену (из /menu)


def buyable_items():
    """Разрешённые к покупке предметы, которые реально отслеживаются (есть в
    TARGETS). Порядок как в TARGETS — стабилен для индексации кнопок меню."""
    return [m for m in TARGETS if m in AUTOBUY_ITEMS]


def get_item_price(mhn):
    with _prices_lock:
        return _item_prices.get(mhn)


def set_item_price(mhn, price):
    """price>0 — задать макс. цену покупки; None/<=0 — убрать (не покупать)."""
    with _prices_lock:
        if price is None or price <= 0:
            _item_prices.pop(mhn, None)
        else:
            _item_prices[mhn] = price


def get_all_prices():
    with _prices_lock:
        return dict(_item_prices)


def clear_all_prices():
    with _prices_lock:
        _item_prices.clear()


def _get_pending():
    with _pending_lock:
        return _pending_item


def _set_pending(item):
    global _pending_item
    with _pending_lock:
        _pending_item = item


def build_purchase_body(item):
    """Тело POST /Exchange/start/v2 — покупка одного предмета по его itemId (GUID)."""
    price = item.get(AUTOBUY_PRICE_FIELD)
    return {
        "botInventoryItems": [
            {"itemId": item["itemId"], "itemSource": 0, "price": price}
        ],
        "userInventoryItems": [],
        "userChestItems": [],
        "balanceValue": price,
        "bonusAmount": None,
    }


def purchase(item):
    """Оформляет покупку. Возвращает (kind, detail):
    kind = ok | auth | fail | error. detail — exchangeId или текст ошибки."""
    body = json.dumps(build_purchase_body(item)).encode()
    url = EXCHANGE_START_URL + PIRATE_STEAMID
    headers = {
        "Authorization": "Bearer " + PIRATE_BEARER,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://pirateswap.com",
        "Referer": "https://pirateswap.com/",
        "User-Agent": HEADERS["User-Agent"],
        "x-session-opened-at": SESSION_OPENED_AT,
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read().decode("utf-8", errors="replace"))
        exch = resp.get("exchangeId")
        return ("ok", exch) if exch else ("fail", resp)
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        if e.code == 401:
            return ("auth", detail)
        return ("fail", f"HTTP {e.code}: {detail}")
    except Exception as e:
        return ("error", str(e))


def _daily_ok():
    """True, если дневной лимит покупок ещё не исчерпан (со сбросом по дате)."""
    global _buy_day, _buy_count
    today = now().date()
    with _buy_lock:
        if _buy_day != today:
            _buy_day, _buy_count = today, 0
        return _buy_count < AUTOBUY_DAILY_LIMIT


def _daily_inc():
    global _buy_count
    with _buy_lock:
        _buy_count += 1


def _pick_candidate(items, cap):
    """Из items выбирает самый дешёвый с itemId и ценой ≤ cap.
    Возвращает (cand|None, cheapest_price|None)."""
    cands = [i for i in items
             if i.get("itemId") and i.get(AUTOBUY_PRICE_FIELD) is not None]
    cands.sort(key=lambda i: float(i[AUTOBUY_PRICE_FIELD]))
    if not cands:
        return None, None
    cand = next((i for i in cands
                 if float(i[AUTOBUY_PRICE_FIELD]) <= cap), None)
    return cand, cands[0][AUTOBUY_PRICE_FIELD]


def try_autobuy(mhn, new_items):
    """Пытается купить самый дешёвый экземпляр кейса в пределах заданной цены.
    Возвращает строку-результат ДЛЯ АДМИНА, либо None (не наш случай)."""
    if not AUTOBUY_ENABLED:
        return None
    # Покупаем только разрешённые кейсы.
    if mhn not in AUTOBUY_ITEMS:
        return None

    # Цена задаётся на КАЖДЫЙ кейс через /menu. Пока цены нет — не покупаем.
    cap = get_item_price(mhn)
    if cap is None:
        log(f"[{mhn}] autobuy: цена не задана — пропуск (/menu)")
        return f"⏳ {mhn} дропнулся, но цена не задана — открой /menu"

    # DRY-RUN: только симуляция на данных детекта, без сетевых запросов покупки.
    if AUTOBUY_DRY_RUN:
        cand, cheapest = _pick_candidate(new_items, cap)
        if cand is None:
            if cheapest is None:
                return None
            return f"🧪 dry-run: {mhn} есть, но {cheapest} выше твоей {cap}"
        log(f"[{mhn}] autobuy DRY-RUN: купил бы за {cand[AUTOBUY_PRICE_FIELD]}")
        log_event("autobuy_dryrun", mhn=mhn, price=cand[AUTOBUY_PRICE_FIELD])
        return f"🧪 dry-run: купил бы {mhn} за {cand[AUTOBUY_PRICE_FIELD]}"

    # БОЕВОЙ режим.
    if not (PIRATE_BEARER and PIRATE_STEAMID):
        return "🛑 автозакупка: не задан токен/SteamID"
    if not _daily_ok():
        return f"🛑 автозакупка: дневной лимит {AUTOBUY_DAILY_LIMIT} исчерпан"

    code = TARGETS.get(mhn)
    # Шаг 1 — «резервация»: авторизованный GET инвентаря (тот же, что у браузера),
    # сервер привяжет показанные предметы к нашей сессии. Заодно берём свежие itemId.
    try:
        items, _ = fetch(mhn, code, authed=True)
    except Exception as e:
        log(f"[{mhn}] autobuy: reserve-GET не удался: {e}")
        log_event("autobuy_fail", mhn=mhn, detail=f"reserve: {e}")
        return f"⚠️ автозакупка: reserve не удался ({e})"

    cand, cheapest = _pick_candidate(items, cap)
    if cand is None:
        if cheapest is None:
            return f"⚠️ {mhn}: предмет уже исчез, не купил"
        log(f"[{mhn}] autobuy: цена {cheapest} > твоей {cap}")
        return f"🛑 {mhn}: цена {cheapest} выше твоей {cap} — не купил"

    price = cand[AUTOBUY_PRICE_FIELD]
    # Шаг 2 — выкуп зарезервированного предмета.
    kind, detail = purchase(cand)
    if kind == "ok":
        _daily_inc()
        log(f"[{mhn}] ✅ куплено за {price} (exchangeId={detail})")
        log_event("autobuy_ok", mhn=mhn, price=price, exchangeId=str(detail))
        return f"✅ Куплено: {mhn} за {price}"
    if kind == "auth":
        log(f"[{mhn}] 🔑 autobuy 401 — токен протух")
        if ADMIN_CHAT:
            tg_send(ADMIN_CHAT, "🔑 Токен автопокупки протух (401). "
                                "Обнови PIRATE_BEARER на Render.")
        log_event("autobuy_auth", mhn=mhn)
        return "🔑 не куплено: токен протух"
    log(f"[{mhn}] ⚠️ autobuy не удалось: {detail}")
    log_event("autobuy_fail", mhn=mhn, detail=str(detail)[:200])
    return f"⚠️ не купил ({mhn}): {str(detail)[:80]}"


# --------------------- Обработка одного предмета ----------------

def poll_one(mhn, code, st):
    try:
        items, foreign = fetch(mhn, code)
        st["backoff"] = max(0, st["backoff"] - 5)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            st["backoff"] = min(st["backoff"] + 60, 600)
            log(f"[{mhn}] 429 rate limit (+{st['backoff']}s)")
        elif e.code in (403, 503):
            st["backoff"] = min(st["backoff"] + 90, 600)
            log(f"[{mhn}] HTTP {e.code} (Cloudflare?) (+{st['backoff']}s)")
            admin_error_alert(f"[{mhn}] HTTP {e.code} (Cloudflare блокирует?)")
        elif 500 <= e.code < 600:
            st["backoff"] = min(st["backoff"] + 30, 300)
            log(f"[{mhn}] HTTP {e.code} (ошибка сервера) (+{st['backoff']}s)")
            admin_error_alert(f"[{mhn}] HTTP {e.code} (ошибка сервера)")
        else:
            st["backoff"] = min(st["backoff"] + 15, 300)
            log(f"[{mhn}] HTTP {e.code} (+{st['backoff']}s)")
        log_event("error", mhn=mhn, http=e.code)
        return None
    except Exception as e:
        st["backoff"] = min(st["backoff"] + 15, 300)
        log(f"[{mhn}] Ошибка: {e} (+{st['backoff']}s)")
        log_event("error", mhn=mhn, msg=str(e))
        return None

    if foreign > 0:
        log(f"[{mhn}] ⚠️ отброшено чужих предметов: {foreign} (алерт по ним НЕ шлю)")
        log_event("mismatch", mhn=mhn, count=foreign)

    cur_ids = {i["assetId"] for i in items}
    count = len(cur_ids)
    is_control = mhn in CONTROL_ITEMS

    # Запоминаем реальное имя для ссылки.
    rn = next((i["name"] for i in items if i.get("name")), None)
    if rn:
        last_market_names[mhn] = rn

    prev_ids = st["prev_ids"]
    new_ids = cur_ids - prev_ids

    if new_ids and not is_control:
        new_items = [i for i in items if i["assetId"] in new_ids]
        market_name = rn or mhn
        log(f"[{mhn}] 🔥 ДРОП: новых {len(new_ids)}, всего {count}")
        log_event("drop", mhn=mhn, count=count, new_count=len(new_ids),
                  asset_ids=sorted(new_ids))
        # Автопокупка — ПЕРВЫМ делом (миллисекунды решают). Идёт на аккаунте
        # админа, поэтому её результат уходит ТОЛЬКО ему (ниже).
        buy_result = try_autobuy(mhn, new_items)
        # Уведомление о дропе — всем с notify=true (двое + админ, если не приглушил),
        # с кнопкой ручной покупки.
        broadcast_drop(build_alert(mhn, items, new_items),
                       reply_markup=buy_keyboard(market_name))
        # Результат закупки — только админу и ВСЕГДА (даже если он приглушил
        # уведомления кнопкой 🔕: так он может получать только закупки).
        if buy_result and ADMIN_CHAT:
            tg_send(ADMIN_CHAT, "🛒 " + buy_result)

    if not cur_ids and prev_ids and not is_control:
        log(f"[{mhn}] закончился (был {len(prev_ids)}).")
        log_event("soldout", mhn=mhn)

    st["prev_ids"] = cur_ids

    with _counts_lock:
        global last_poll_ts
        last_counts[mhn] = count
        last_poll_ts = now()

    return count


# ------------------------ Поток-поллер -------------------------

def make_states():
    return {mhn: {"prev_ids": set(), "backoff": 0} for mhn in TARGETS}


def initial_scan(states):
    """Тихий baseline: фиксируем текущие assetId БЕЗ алертов (чтобы не спамить
    при каждом рестарте Render). Алерты — только на дропы ПОСЛЕ старта."""
    log("Тихий стартовый скан (baseline, без алертов)...")
    for mhn, code in TARGETS.items():
        try:
            items, foreign = fetch(mhn, code)
            ids = {i["assetId"] for i in items}
            states[mhn]["prev_ids"] = ids
            rn = next((i["name"] for i in items if i.get("name")), None)
            if rn:
                last_market_names[mhn] = rn
            with _counts_lock:
                last_counts[mhn] = len(ids)
            extra = f" (чужих отброшено: {foreign})" if foreign else ""
            tag = " [контроль]" if mhn in CONTROL_ITEMS else ""
            log(f"[{mhn}] baseline: {len(ids)} шт.{extra}{tag}")
        except Exception as e:
            log(f"[{mhn}] стартовый запрос не удался: {e}")
        time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))
    with _counts_lock:
        global last_poll_ts
        last_poll_ts = now()


def poller_loop():
    log("Поток-поллер запущен. Цели: " + ", ".join(TARGETS))
    log("Пример URL: " + build_url(*next(iter(TARGETS.items()))))
    states = make_states()
    initial_scan(states)
    log_event("startup")
    if not AUTOBUY_ENABLED:
        log("Автопокупка: ВЫКЛ (только алерты).")
    else:
        mode = "DRY-RUN (симуляция)" if AUTOBUY_DRY_RUN else "БОЕВОЙ (тратит деньги!)"
        items = ", ".join(buyable_items()) or "(нет валидных — проверь AUTOBUY_ITEMS)"
        log(f"Автопокупка: ВКЛ [{mode}] предметы={items} "
            f"price_field={AUTOBUY_PRICE_FIELD} daily_limit={AUTOBUY_DAILY_LIMIT} "
            f"token={'есть' if PIRATE_BEARER else 'НЕТ'} "
            f"steamid={'есть' if PIRATE_STEAMID else 'НЕТ'} "
            f"| цены НЕ заданы (сброс при рестарте), жду /menu")
        # Предупредим, если в AUTOBUY_ITEMS есть имена, которых нет в TARGETS.
        bad = [m for m in AUTOBUY_ITEMS if m not in TARGETS]
        if bad:
            log(f"⚠️ AUTOBUY_ITEMS не совпадают с TARGETS (не будут покупаться): {bad}")
        # После рестарта цены сброшены — напоминаем админу выставить их.
        if ADMIN_CHAT:
            tg_send(ADMIN_CHAT, f"🛒 Автозакупка включена [{mode}], кейсы: {items}.\n"
                                f"⏳ Цены сброшены (после рестарта). "
                                f"Выставь их: /menu")
    if ADMIN_CHAT:
        tg_send(ADMIN_CHAT, "🟢 Бот запущен. Мониторинг идёт постоянно.\nСлежу за:\n• "
                + "\n• ".join(m for m in TARGETS if m not in CONTROL_ITEMS))
    real_targets = [(m, c) for m, c in TARGETS.items() if m not in CONTROL_ITEMS]
    control_targets = [(m, c) for m, c in TARGETS.items() if m in CONTROL_ITEMS]

    def poll_target(mhn, code):
        if states[mhn]["backoff"] > 0:
            time.sleep(states[mhn]["backoff"])
        try:
            poll_one(mhn, code, states[mhn])
        except Exception as e:
            log(f"[{mhn}] непойманная ошибка в poll_one: {e}")
        time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))

    last_hb = now()
    last_control = now()   # контроль только что опрошен в initial_scan
    idx = 0

    while True:
        # 1. Боевые предметы — round-robin, по одному за проход (быстрый детект).
        if real_targets:
            mhn, code = real_targets[idx % len(real_targets)]
            idx += 1
            poll_target(mhn, code)

        # 2. Контрольный Prisma — редко, только как индикатор «бот видит сток».
        if control_targets and (now() - last_control).total_seconds() >= CONTROL_EVERY_SEC:
            for mhn, code in control_targets:
                poll_target(mhn, code)
            last_control = now()

        # 3. Heartbeat админу.
        if HEARTBEAT_HOURS > 0 and ADMIN_CHAT:
            if (now() - last_hb).total_seconds() / 3600 >= HEARTBEAT_HOURS:
                with _counts_lock:
                    summary = ", ".join(
                        f"{m}: {last_counts.get(m) if last_counts.get(m) is not None else '?'}"
                        for m in TARGETS)
                tg_send(ADMIN_CHAT, "❤️ Бот жив. В наличии — " + summary)
                log_event("heartbeat")
                last_hb = now()


# ----------------------- Поток Telegram ------------------------

WELCOME = (
    "🏴‍☠️ PirateSwap-бот (закрытый).\n\n"
    "Я слежу за появлением кейсов и пришлю алерт, как только что-то дропнется. "
    "Мониторинг работает всегда — кнопки ниже включают/выключают уведомления "
    "ЛИЧНО для тебя, на других и на мониторинг не влияют.\n\n"
    "Команды:\n"
    "• /status — что сейчас в наличии + твой статус\n"
    "• кнопки 🔕/🔔 — выкл/вкл уведомления для себя"
)


def autobuy_state_line():
    """Многострочное описание состояния автозакупки (цены по кейсам) — для админа."""
    if not AUTOBUY_ENABLED:
        return "🛒 Автозакупка: ВЫКЛ в конфиге (AUTOBUY_ENABLED=0)"
    mode = "DRY-RUN (симуляция)" if AUTOBUY_DRY_RUN else "БОЕВОЙ (реальные покупки!)"
    items = buyable_items()
    prices = get_all_prices()
    lines = [f"🛒 Автозакупка [{mode}]"]
    if not items:
        lines.append("⚠️ AUTOBUY_ITEMS пуст или имена не совпадают с отслеживаемыми "
                     "(см. TARGETS).")
    for m in items:
        p = prices.get(m)
        lines.append(f"• {m}: {'≤ ' + str(p) if p else '⏳ цена не задана'}")
    lines.append("Задать цены: /menu")
    return "\n".join(lines)


def _parse_number(s):
    """'0,31' / ' 0.31$ ' -> 0.31. Возвращает float>0 или None."""
    raw = s.strip().replace(",", ".").replace("$", "").strip()
    try:
        v = float(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def menu_text():
    return ("🛒 Меню автозакупки.\n"
            "Нажми кейс и пришли цену (напр. 0.31). Бот купит этот кейс, когда он "
            "дропнется по цене ≤ заданной. /cancel — отменить ввод.")


def menu_keyboard():
    items = buyable_items()
    prices = get_all_prices()
    rows = []
    for idx, m in enumerate(items):
        p = prices.get(m)
        label = f"{m} — ≤ {p}" if p else f"{m} — не задано"
        rows.append([{"text": label, "callback_data": f"pm:{idx}"}])
    rows.append([{"text": "🔴 Сбросить все цены", "callback_data": "pm:clear"}])
    return {"inline_keyboard": rows}


def _cmd_menu(chat_id):
    if chat_id != ADMIN_CHAT:
        tg_send(chat_id, "Эта команда доступна только админу.")
        return
    if not AUTOBUY_ENABLED:
        tg_send(chat_id, "Автозакупка выключена в конфиге (AUTOBUY_ENABLED=0).")
        return
    if not buyable_items():
        tg_send(chat_id, "AUTOBUY_ITEMS пуст или имена не совпадают с отслеживаемыми "
                         "кейсами. Проверь переменную на Render.")
        return
    tg_send(chat_id, menu_text(), reply_markup=menu_keyboard())


def _handle_price_input(chat_id, item, text):
    """Админ прислал цену для выбранного в /menu кейса."""
    price = _parse_number(text)
    if price is None:
        tg_send(chat_id, f"Не понял цену «{text}». Пришли число, напр. 0.31, или /cancel.")
        return
    if AUTOBUY_MAX_PRICE > 0 and price > AUTOBUY_MAX_PRICE:
        tg_send(chat_id, f"{price} выше жёсткого потолка {AUTOBUY_MAX_PRICE}. Понизь.")
        return
    set_item_price(item, price)
    _set_pending(None)
    log_event("autobuy_price_set", item=item, price=price)
    tg_send(chat_id, f"✅ {item}: буду покупать по цене ≤ {price}",
            reply_markup=menu_keyboard())


def _cmd_buyoff(chat_id):
    if chat_id != ADMIN_CHAT:
        tg_send(chat_id, "Эта команда доступна только админу.")
        return
    clear_all_prices()
    _set_pending(None)
    log_event("autobuy_disarmed")
    tg_send(chat_id, "⏸ Все цены сброшены — автозакупка ничего не покупает. "
                     "Задать заново: /menu")


def _cmd_buystatus(chat_id):
    if chat_id != ADMIN_CHAT:
        tg_send(chat_id, "Эта команда доступна только админу.")
        return
    tg_send(chat_id, autobuy_state_line())


def status_text(chat_id):
    with _counts_lock:
        counts = dict(last_counts)
        poll_ts = last_poll_ts
    age = ""
    if poll_ts:
        mins = (now() - poll_ts).total_seconds() / 60
        age = f" (обновлено {int(mins)} мин назад)" if mins >= 1 else " (только что)"

    lines = [f"📊 Наличие{age}:"]
    for mhn in TARGETS:
        c = counts.get(mhn)
        shown = "?" if c is None else str(c)
        if mhn in CONTROL_ITEMS:
            mark = " ✅ бот видит сток" if (c or 0) > 0 else " ⚠️ контроль пуст!"
            lines.append(f"• {mhn}: {shown}{mark} (контроль)")
        else:
            lines.append(f"• {mhn}: {shown}")

    on = get_notify(chat_id)
    lines.append("")
    lines.append(f"🔔 Твои уведомления: {'ВКЛ' if on else 'ВЫКЛ'}")
    if chat_id == ADMIN_CHAT and AUTOBUY_ENABLED:
        lines.append("")
        lines.append(autobuy_state_line())
    return "\n".join(lines)


def handle_message(msg):
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if chat_id not in WHITELIST:
        log(f"⛔ сообщение от чужого chat_id={chat_id} ({text[:40]!r}) — игнор")
        log_event("denied", chat_id=chat_id)
        return

    # Ввод цены из /menu: админ прислал не-команду, пока ждём цену для кейса.
    if chat_id == ADMIN_CHAT and text and not text.startswith("/"):
        pend = _get_pending()
        if pend is not None:
            _handle_price_input(chat_id, pend, text)
            return

    cmd = text.split()[0].lower() if text else ""
    if cmd.startswith("/start"):
        set_notify(chat_id, True)   # активация чата; уведомления по умолчанию вкл
        # Сначала убираем старую reply-клавиатуру от прежнего бота...
        tg_send(chat_id, "♻️ Обновляю бота, убираю старую клавиатуру.",
                reply_markup=REMOVE_KEYBOARD)
        # ...затем приветствие с актуальными inline-кнопками.
        tg_send(chat_id, WELCOME, reply_markup=main_keyboard())
    elif cmd.startswith("/status"):
        tg_send(chat_id, status_text(chat_id), reply_markup=main_keyboard())
    elif cmd.startswith("/stop"):
        set_notify(chat_id, False)
        tg_send(chat_id, "🔕 Уведомления выключены. Мониторинг продолжается.",
                reply_markup=main_keyboard())
    elif cmd.startswith("/go") or cmd.startswith("/resume"):
        set_notify(chat_id, True)
        tg_send(chat_id, "🔔 Уведомления включены.", reply_markup=main_keyboard())
    elif cmd in ("/menu", "/buy", "/prices"):
        _cmd_menu(chat_id)
    elif cmd == "/cancel":
        _set_pending(None)
        tg_send(chat_id, "Отменено.")
    elif cmd in ("/buyoff", "/stopbuy"):
        _cmd_buyoff(chat_id)
    elif cmd == "/buystatus":
        _cmd_buystatus(chat_id)
    else:
        tg_send(chat_id, "Команды: /status, /menu (закупки), или кнопки ниже.",
                reply_markup=main_keyboard())


def handle_callback(cb):
    cb_id = cb.get("id")
    msg = cb.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    data = cb.get("data") or ""

    if chat_id not in WHITELIST:
        tg_answer_callback(cb_id, "Доступ закрыт")
        log(f"⛔ callback от чужого chat_id={chat_id} — игнор")
        return

    # Кнопки меню автозакупки (только админ).
    if data.startswith("pm:"):
        if chat_id != ADMIN_CHAT:
            tg_answer_callback(cb_id, "Только для админа")
            return
        arg = data[3:]
        if arg == "clear":
            clear_all_prices()
            _set_pending(None)
            tg_answer_callback(cb_id, "Все цены сброшены")
            tg_edit_text(chat_id, message_id, menu_text(), reply_markup=menu_keyboard())
            return
        items = buyable_items()
        try:
            item = items[int(arg)]
        except (ValueError, IndexError):
            tg_answer_callback(cb_id, "Устарело, открой /menu заново")
            return
        _set_pending(item)
        tg_answer_callback(cb_id, f"Пришли цену для {item}")
        tg_send(chat_id, f"Пришли цену для «{item}» (напр. 0.31). /cancel — отмена.")
        return

    if data == "n_off":
        set_notify(chat_id, False)
        tg_answer_callback(cb_id, "Уведомления выключены")
    elif data == "n_on":
        set_notify(chat_id, True)
        tg_answer_callback(cb_id, "Уведомления включены")
    else:
        tg_answer_callback(cb_id)
        return

    on = get_notify(chat_id)
    new_text = f"🔔 Твои уведомления: {'ВКЛ' if on else 'ВЫКЛ'}\nМониторинг идёт постоянно."
    if not tg_edit_text(chat_id, message_id, new_text, reply_markup=main_keyboard()):
        tg_send(chat_id, new_text, reply_markup=main_keyboard())


def telegram_loop():
    if not TG_TOKEN:
        log("⚠️ PIRATE_TG_TOKEN не задан — поток Telegram не запускаю.")
        return
    log("Поток Telegram (getUpdates long-polling) запущен.")
    offset = None
    while True:
        try:
            params = {"timeout": 25, "allowed_updates": json.dumps(["message", "callback_query"])}
            if offset is not None:
                params["offset"] = offset
            updates = tg_api("getUpdates", params, timeout=40)
            if not updates:
                if updates is None:
                    time.sleep(3)   # ошибка API — короткая пауза
                continue
            for upd in updates:
                offset = upd["update_id"] + 1
                try:
                    if "message" in upd:
                        handle_message(upd["message"])
                    elif "callback_query" in upd:
                        handle_callback(upd["callback_query"])
                except Exception as e:
                    log(f"⚠️ ошибка обработки апдейта: {e}")
        except Exception as e:
            log(f"⚠️ telegram_loop сбой: {e}")
            time.sleep(5)


# ------------------------- HTTP /health ------------------------

class HealthHandler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/health"):
            self._send(200, "OK")
        elif self.path == "/":
            with _counts_lock:
                poll_ts = last_poll_ts
                counts = dict(last_counts)
            age = "никогда" if not poll_ts else poll_ts.strftime("%H:%M:%S")
            body = "PirateSwap bot alive. Last poll: " + age + "\n" + \
                   "\n".join(f"{m}: {counts.get(m)}" for m in TARGETS)
            self._send(200, body)
        else:
            self._send(404, "not found")

    def log_message(self, *args):
        pass   # глушим access-лог http.server


def run_http():
    srv = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), HealthHandler)
    log(f"HTTP-сервер слушает 0.0.0.0:{HTTP_PORT} (/health -> 200).")
    srv.serve_forever()


# ------------------------- self-test ---------------------------

def _selftest():
    """Проверка ключевой логики на фейковых данных (без сети)."""
    print("== self-test ==")
    ok = True

    # 1. build_url содержит v2 и ровно один код.
    url = build_url("Fracture Case", -87706406)
    assert "/inventory/v2/ExchangerInventory" in url, "нет v2 в URL"
    assert "marketHashNameHashCodes=-87706406" in url, "код не подставился"
    assert url.count("marketHashNameHashCodes") == 1
    print("  [ok] build_url: v2 + один код")

    # 2. Анти-ложная защита: чужой marketNameHashCode отбрасывается.
    fake = {
        "items": [
            {"assetId": "A1", "marketNameHashCode": -87706406, "price": 5.0,
             "marketHashName": "Fracture Case"},
            {"assetId": "X9", "marketNameHashCode": 111111, "price": 1.0,
             "marketHashName": "Чужой предмет"},   # ДОЛЖЕН быть отброшен
        ]
    }
    orig = urllib.request.urlopen

    class _Resp:
        def __init__(self, b): self._b = b
        def read(self): return self._b
        def __enter__(self): return self
        def __exit__(self, *a): return False

    urllib.request.urlopen = lambda *a, **k: _Resp(json.dumps(fake).encode())
    try:
        items, foreign = fetch("Fracture Case", -87706406)
    finally:
        urllib.request.urlopen = orig
    assert foreign == 1, f"чужой не отброшен (foreign={foreign})"
    assert {i['assetId'] for i in items} == {"A1"}, "остались не те id"
    print("  [ok] анти-ложная защита: чужой hash отброшен")

    # 3. Детект дропа по разнице множеств.
    st = {"prev_ids": {"A1"}, "backoff": 0}
    cur = {"A1", "A2"}
    new = cur - st["prev_ids"]
    assert new == {"A2"}, "детект новых assetId сломан"
    print("  [ok] детект: новые assetId = дроп")

    # 4. Контрольный предмет не порождает дроп-алерт.
    assert "Prisma Case" in CONTROL_ITEMS
    print("  [ok] Prisma — контрольный (без дроп-алертов)")

    # 5. build_alert: tradableAfter только если есть; цена в тексте.
    items_ta = [{"id": "A2", "price": 7.5, "tradableAfter": "2026-07-01",
                 "name": "Fracture Case"}]
    a = build_alert("Fracture Case", items_ta, items_ta)
    assert "tradableAfter" in a and "2026-07-01" in a
    assert "7.5" in a, "цена не попала в алерт"
    items_no = [{"id": "A3", "price": 7.5, "tradableAfter": None,
                 "name": "Fracture Case"}]
    b = build_alert("Fracture Case", items_no, items_no)
    assert "tradableAfter" not in b, "tradableAfter не должен упоминаться"
    print("  [ok] build_alert: tradableAfter условно, цена есть")

    # 5b. Ссылка на покупку — теперь в URL-кнопке.
    kb = buy_keyboard("Fracture Case")
    btn = kb["inline_keyboard"][0][0]
    assert btn["text"] == "🛒 Купить"
    assert btn["url"].startswith("https://pirateswap.com/exchanger?mhn=")
    print("  [ok] кнопка «Купить»: URL на предмет")

    # 5c. Тело покупки: itemId (GUID), цена, balanceValue.
    item = {"assetId": "A2", "itemId": "guid-123", "price": 0.43,
            "storePrice": 0.40, "name": "Recoil Case"}
    body = build_purchase_body(item)
    assert body["botInventoryItems"][0]["itemId"] == "guid-123"
    assert body["botInventoryItems"][0]["itemSource"] == 0
    assert body["balanceValue"] == body["botInventoryItems"][0]["price"]
    print("  [ok] build_purchase_body: itemId + цена + balanceValue")

    # 5d. Автозакупка: цена на кейс, потолок, whitelist предметов, kill-switch.
    global AUTOBUY_ENABLED, AUTOBUY_DRY_RUN
    _sv = (AUTOBUY_ENABLED, AUTOBUY_DRY_RUN)
    ritem = dict(item, name="Revolution Case")     # цена 0.43, в списке разрешённых
    try:
        AUTOBUY_ENABLED, AUTOBUY_DRY_RUN = True, True
        clear_all_prices()                         # цена не задана -> НЕ покупает
        r0 = try_autobuy("Revolution Case", [ritem])
        assert r0 and "цена не задана" in r0, f"no-price не сработал: {r0}"
        set_item_price("Revolution Case", 1.0)     # 0.43 <= 1.0 -> dry-run покупка
        r = try_autobuy("Revolution Case", [ritem])
        assert r and "dry-run" in r, f"dry-run не сработал: {r}"
        set_item_price("Revolution Case", 0.1)     # 0.43 > 0.1 -> не купит
        r2 = try_autobuy("Revolution Case", [ritem])
        assert r2 and "выше" in r2, f"потолок цены не сработал: {r2}"
        set_item_price("Fracture Case", 5.0)       # даже с ценой — не в AUTOBUY_ITEMS
        assert try_autobuy("Fracture Case", [ritem]) is None
        AUTOBUY_ENABLED = False                     # выключено -> None
        assert try_autobuy("Revolution Case", [ritem]) is None
    finally:
        AUTOBUY_ENABLED, AUTOBUY_DRY_RUN = _sv
        clear_all_prices()
    print("  [ok] автозакупка: цена/кейс, потолок, whitelist, kill-switch")

    # 6. notify-флаги: чужой chat_id не добавляется.
    global notify_state
    notify_state = {str(c): {"notify": True} for c in WHITELIST}
    assert set_notify(99999, False) is False, "чужой chat_id не должен меняться"
    assert set_notify(ADMIN_CHAT, False) is True
    assert get_notify(ADMIN_CHAT) is False
    set_notify(ADMIN_CHAT, True)
    print("  [ok] notify: белый список закрыт, флаг переключается")

    print("== self-test passed ==" if ok else "== FAILED ==")


# ------------------------------ main ---------------------------

def run_once():
    log("Один проход опроса (--once), без бота.")
    states = make_states()
    for mhn, code in TARGETS.items():
        try:
            items, foreign = fetch(mhn, code)
            extra = f"  | чужих отброшено: {foreign}" if foreign else ""
            log(f"[{mhn}] в наличии: {len(items)} шт.{extra}")
        except Exception as e:
            log(f"[{mhn}] ошибка: {e}")
        time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "--selftest":
        _selftest()
        return
    if arg == "--once":
        run_once()
        return

    if not TG_TOKEN:
        log("❌ PIRATE_TG_TOKEN не задан. Установи переменную окружения и перезапусти.")
        sys.exit(1)

    state_load()

    threading.Thread(target=poller_loop, name="poller", daemon=True).start()
    threading.Thread(target=telegram_loop, name="telegram", daemon=True).start()

    try:
        run_http()   # блокирующий, держит процесс живым
    except KeyboardInterrupt:
        log("Остановлено пользователем.")


if __name__ == "__main__":
    main()

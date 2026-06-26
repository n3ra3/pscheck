#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PirateSwap drop monitor (мульти-предмет, эндпоинт v2).

Следит за несколькими предметами и шлёт уведомление в Telegram, как только
предмет появляется в наличии. Фильтрация — по marketHashNameHashCodes (точный
числовой код предмета), плюс защита от ложных срабатываний.

Зависимостей нет — только стандартная библиотека Python 3.8+.

Запуск:
    python pirateswap_monitor.py              # мониторинг
    python pirateswap_monitor.py --get-chat   # узнать chat_id
    python pirateswap_monitor.py --test        # тестовое сообщение в ТГ
    python pirateswap_monitor.py --once        # один проход (для проверки), без цикла
"""

import os
import sys
import json
import time
import random
import sqlite3
import datetime as dt
import urllib.parse
import urllib.request
import urllib.error

# ============================ КОНФИГ ============================

# Предметы: имя -> его marketHashNameHashCode (число из URL фильтра).
# Чтобы добавить новый: открой фильтр по нему на сайте, найди в Network запрос
# v2/ExchangerInventory и спиши значение marketHashNameHashCodes.
TARGETS = {
    "Sealed Dead Hand Terminal": -1818601513,
    "Fracture Case":             -87706406,
    "Kilowatt Case":             1757559884,
    "Revolution Case":           1299851513,
    # Контрольный предмет: он всегда в наличии. Если по нему стабильно
    # показывает count > 0 — значит бот реально видит сток и работает.
    # Только один код (обычный Prisma Case) — несколько кодов сразу давали 500.
    "Prisma Case":               -2067952841,
}

# Правильный эндпоинт (с v2!)
BASE_URL = "https://web.pirateswap.com/inventory/v2/ExchangerInventory"

# Базовые параметры (searchPhrase и marketHashNameHashCodes добавляются на предмет)
PARAMS = {
    "orderBy": "price",
    "sortOrder": "DESC",
    "page": "1",
    "results": "40",
}

# --- Частота (Cloudflare у них РЕАГИРУЕТ — держим спокойный темп) ---
# Пауза после полного обхода всех предметов (секунды, с джиттером).
POLL_MIN = 30
POLL_MAX = 50
# Пауза между запросами разных предметов внутри обхода.
ITEM_GAP_MIN = 3.0
ITEM_GAP_MAX = 6.0

# --- Защита от ложных алертов ---
# Если фильтр вдруг отвалится, API вернёт чужие предметы. Поэтому СВЕРЯЕМ,
# что у пришедших предметов marketNameHashCode совпадает с ожидаемым кодом.
# Чужие предметы отбрасываются и пишется предупреждение в лог.
STRICT_HASH_CHECK = True

# Контрольные предметы: всегда в наличии, нужны только как индикатор "бот видит сток".
# По ним НЕ шлём ни дроп-алерты, ни напоминания — они только попадают в heartbeat.
CONTROL_ITEMS = {"Prisma Case"}

# --- Telegram (через переменные окружения, НЕ хардкодить) ---
TG_TOKEN = os.environ.get("PIRATE_TG_TOKEN", "").strip()
TG_CHAT  = os.environ.get("PIRATE_TG_CHAT", "").strip()

REALERT_COOLDOWN_MIN = 30      # напоминание "ещё в наличии" не чаще, чем раз в N мин
HEARTBEAT_HOURS = 6            # пинг "жив" раз в N часов (0 = выкл)

DB_PATH = "pirateswap_drops.db"

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


def now():
    return dt.datetime.now()


def ts():
    return now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{ts()}] {msg}", flush=True)


def item_link(mhn):
    return "https://pirateswap.com/exchanger?mhn=" + urllib.parse.quote(mhn)


# ----------------------------- БД ------------------------------

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        TEXT NOT NULL,
            kind      TEXT NOT NULL,      -- drop | soldout | heartbeat | error | mismatch
            mhn       TEXT,
            count     INTEGER,
            new_count INTEGER,
            asset_ids TEXT
        )
    """)
    con.commit()
    return con


def db_event(con, kind, mhn=None, count=None, new_count=None, asset_ids=None):
    con.execute(
        "INSERT INTO events (ts, kind, mhn, count, new_count, asset_ids) "
        "VALUES (?,?,?,?,?,?)",
        (ts(), kind, mhn, count, new_count,
         json.dumps(sorted(asset_ids)) if asset_ids else None),
    )
    con.commit()


# -------------------------- Telegram ---------------------------

def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        log("⚠️  TG не настроен — только консоль. Текст: " + text)
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TG_CHAT, "text": text,
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=20) as r:
            r.read()
        return True
    except Exception as e:
        log(f"⚠️  Ошибка отправки в ТГ: {e}")
        return False


def tg_get_chat_id():
    if not TG_TOKEN:
        print("Сначала задай PIRATE_TG_TOKEN (токен от @BotFather).")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        print(f"Ошибка getUpdates: {e}")
        return
    results = data.get("result", [])
    if not results:
        print("Апдейтов нет. Напиши боту любое сообщение и запусти снова.")
        return
    seen = {}
    for upd in results:
        chat = (upd.get("message") or upd.get("edited_message") or {}).get("chat")
        if chat:
            seen[chat["id"]] = (chat.get("username") or chat.get("title")
                                or chat.get("first_name", ""))
    print("Найденные chat_id:")
    for cid, name in seen.items():
        print(f"  {cid}   ({name})")
    print('\nЭкспортируй: export PIRATE_TG_CHAT="<chat_id>"')


# --------------------------- Запросы ---------------------------

def codes_list(code):
    """Приводит код к списку int: число -> [число], список -> список."""
    if isinstance(code, (list, tuple)):
        return [int(c) for c in code]
    return [int(code)]


def build_url(mhn, code):
    params = dict(PARAMS)
    params["searchPhrase"] = mhn
    params["marketHashNameHashCodes"] = ",".join(str(c) for c in codes_list(code))
    return BASE_URL + "?" + urllib.parse.urlencode(params, safe=",")


def fetch(mhn, code):
    """
    Возвращает (asset_ids:set, count:int, foreign:int).
    foreign — сколько предметов отброшено как ЧУЖИЕ (не тот hash code).
    Бросает исключение при сетевой/HTTP ошибке.
    """
    url = build_url(mhn, code)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    items = data.get("items") or []

    allowed = set(codes_list(code))
    ids = set()
    foreign = 0
    for it in items:
        # ЗАЩИТА: сверяем код предмета с ожидаемым (любым из allowed)
        if STRICT_HASH_CHECK:
            ihash = it.get("marketNameHashCode")
            if ihash is not None and int(ihash) not in allowed:
                foreign += 1
                continue
        aid = it.get("assetId")
        if aid is None:
            aid = it.get("id")
        if aid is not None:
            ids.add(str(aid))
    return ids, len(ids), foreign


# --------------------- Обработка одного предмета ----------------

def poll_one(con, mhn, code, state):
    try:
        cur_ids, count, foreign = fetch(mhn, code)
        state["backoff"] = max(0, state["backoff"] - 5)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            state["backoff"] = min(state["backoff"] + 60, 600)
            log(f"[{mhn}] 429 rate limit (+{state['backoff']}s)")
        elif e.code in (403, 503):
            state["backoff"] = min(state["backoff"] + 90, 600)
            log(f"[{mhn}] HTTP {e.code} (Cloudflare?) (+{state['backoff']}s)")
        elif 500 <= e.code < 600:
            state["backoff"] = min(state["backoff"] + 30, 300)
            log(f"[{mhn}] HTTP {e.code} (ошибка сервера) (+{state['backoff']}s)")
        else:
            state["backoff"] = min(state["backoff"] + 15, 300)
            log(f"[{mhn}] HTTP {e.code} (+{state['backoff']}s)")
        db_event(con, "error", mhn=mhn)
        return None
    except Exception as e:
        state["backoff"] = min(state["backoff"] + 15, 300)
        log(f"[{mhn}] Ошибка: {e} (+{state['backoff']}s)")
        db_event(con, "error", mhn=mhn)
        return None

    # Если пришли чужие предметы — это тревожный знак (фильтр поехал)
    if foreign > 0:
        log(f"[{mhn}] ⚠️ отброшено чужих предметов: {foreign} "
            f"(фильтр по hash сработал, алерт по ним НЕ шлю)")
        db_event(con, "mismatch", mhn=mhn, count=foreign)

    prev_ids = state["prev_ids"]
    new_ids = cur_ids - prev_ids
    is_control = mhn in CONTROL_ITEMS

    if new_ids and not is_control:
        msg = (f"🔥 ДРОП! {mhn}\n"
               f"Новых: {len(new_ids)} | Всего: {count}\n"
               f"{item_link(mhn)}")
        log(msg.replace("\n", " | "))
        tg_send(msg)
        db_event(con, "drop", mhn=mhn, count=count,
                 new_count=len(new_ids), asset_ids=cur_ids)
        state["last_alert"] = now()

    elif count > 0 and state["last_alert"] is not None and not is_control:
        mins = (now() - state["last_alert"]).total_seconds() / 60
        if mins >= REALERT_COOLDOWN_MIN:
            tg_send(f"⏳ {mhn} всё ещё в наличии: {count} шт.")
            state["last_alert"] = now()

    if not cur_ids and prev_ids and not is_control:
        log(f"[{mhn}] закончился (был {len(prev_ids)}).")
        db_event(con, "soldout", mhn=mhn, count=0)
        state["last_alert"] = None

    state["prev_ids"] = cur_ids
    return count


# ------------------------ Основной цикл ------------------------

def make_states():
    return {mhn: {"prev_ids": set(), "last_alert": None, "backoff": 0}
            for mhn in TARGETS}


def initial_scan(con, states, alert_on_start):
    for mhn, code in TARGETS.items():
        try:
            ids, count, foreign = fetch(mhn, code)
            states[mhn]["prev_ids"] = ids
            extra = f" (отброшено чужих: {foreign})" if foreign else ""
            tag = " [контрольный]" if mhn in CONTROL_ITEMS else ""
            log(f"[{mhn}] старт: {count} шт.{extra}{tag}")
            if count > 0 and alert_on_start and mhn not in CONTROL_ITEMS:
                tg_send(f"⚠️ {mhn} уже в наличии на старте: {count} шт.\n{item_link(mhn)}")
                db_event(con, "drop", mhn=mhn, count=count,
                         new_count=count, asset_ids=ids)
                states[mhn]["last_alert"] = now()
        except Exception as e:
            log(f"[{mhn}] первичный запрос не удался: {e}")
        time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))


def monitor():
    con = db_init()
    log("Старт мониторинга. Цели: " + ", ".join(TARGETS))
    log("Пример URL: " + build_url(next(iter(TARGETS)), next(iter(TARGETS.values()))))
    if not (TG_TOKEN and TG_CHAT):
        log("⚠️  Telegram не настроен — алерты только в консоли (--get-chat).")
    else:
        tg_send("🟢 Монитор запущен.\nСлежу за:\n• " + "\n• ".join(TARGETS))

    states = make_states()
    initial_scan(con, states, alert_on_start=True)
    last_heartbeat = now()

    while True:
        counts = {}
        for mhn, code in TARGETS.items():
            if states[mhn]["backoff"] > 0:
                time.sleep(states[mhn]["backoff"])
            counts[mhn] = poll_one(con, mhn, code, states[mhn])
            time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))

        if HEARTBEAT_HOURS > 0:
            hrs = (now() - last_heartbeat).total_seconds() / 3600
            if hrs >= HEARTBEAT_HOURS:
                summary = ", ".join(
                    f"{m}: {counts.get(m) if counts.get(m) is not None else '?'}"
                    for m in TARGETS)
                tg_send("❤️ Монитор жив. В наличии — " + summary)
                db_event(con, "heartbeat")
                last_heartbeat = now()

        time.sleep(random.uniform(POLL_MIN, POLL_MAX))


def run_once():
    """Один проход без цикла — для проверки, что фильтр и URL работают."""
    con = db_init()
    log("Проверочный проход (--once). НЕ шлю стартовые алерты, только показываю наличие.")
    states = make_states()
    for mhn, code in TARGETS.items():
        try:
            ids, count, foreign = fetch(mhn, code)
            extra = f"  | отброшено чужих: {foreign}" if foreign else ""
            log(f"[{mhn}] в наличии: {count} шт.{extra}")
        except Exception as e:
            log(f"[{mhn}] ошибка: {e}")
        time.sleep(random.uniform(ITEM_GAP_MIN, ITEM_GAP_MAX))
    log("Готово. Если у всех 0 (а на сайте их нет) — фильтр работает правильно.")


# ------------------------------ CLI ----------------------------

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "--get-chat":
        tg_get_chat_id()
    elif arg == "--test":
        ok = tg_send("✅ Тест от PirateSwap-монитора.")
        print("Отправлено." if ok else "Не отправлено — проверь токен/chat_id.")
    elif arg == "--once":
        run_once()
    else:
        try:
            monitor()
        except KeyboardInterrupt:
            log("Остановлено пользователем.")


if __name__ == "__main__":
    main()
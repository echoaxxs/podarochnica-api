import os
import json
import hashlib
import hmac
import asyncio
import random
import uuid
import time
import httpx
import gspread
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)
from aiogram.filters import Command
from aiogram.enums import ParseMode

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

print("=" * 50)
print("🚀 ПОДАРОЧНИЦА v11.0 — FIXED GIFTS MAPPING")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

REQUIRED_CHANNELS_RAW = os.getenv("REQUIRED_CHANNELS", "")
REQUIRED_CHANNELS = []
CHANNEL_LINKS = {}

for item in REQUIRED_CHANNELS_RAW.split(","):
    item = item.strip()
    if not item:
        continue
    parts = item.split("|")
    channel_id = parts[0].strip()
    invite_link = parts[1].strip() if len(parts) > 1 else None
    REQUIRED_CHANNELS.append(channel_id)
    if invite_link:
        CHANNEL_LINKS[channel_id] = invite_link

SENDERS = ["@echoaxxs", "@bogclm", "@bogclm и @echoaxxs"]
SIGNATURE_COSTS = {"@echoaxxs": 2, "@bogclm": 2, "@bogclm и @echoaxxs": 5}

PITY_THRESHOLD = 20

# ===== ЗАХАРДКОЖЕННЫЕ ПОДАРКИ С РЕАЛЬНЫМИ telegram_gift_id =====
# ВАЖНО: Названия подбери сам, посмотрев какой подарок приходит!
# Чтобы узнать: купи каждый себе и посмотри что пришло

HARDCODED_GIFTS = [
    # 15 Stars (1 подарок)
    {
        "id": "gift_15_1",
        "title": "Синяя Звезда",  # <-- поменяй на реальное название
        "emoji": "⭐",
        "price": 15,
        "telegram_gift_id": "5170145012310081615"
    },
    
    # 25 Stars (2 подарка)
    {
        "id": "gift_25_1",
        "title": "Зелёный Подарок",  # <-- поменяй
        "emoji": "🎁",
        "price": 25,
        "telegram_gift_id": "5168103777563050263"
    },
    {
        "id": "gift_25_2",
        "title": "Розовое Сердце",  # <-- поменяй
        "emoji": "💝",
        "price": 25,
        "telegram_gift_id": "5170250947678437525"
    },
    
    # 50 Stars (5 подарков)
    {
        "id": "gift_50_1",
        "title": "Торт",  # <-- поменяй
        "emoji": "🎂",
        "price": 50,
        "telegram_gift_id": "5170144170496491616"
    },
    {
        "id": "gift_50_2",
        "title": "Ракета",  # <-- поменяй
        "emoji": "🚀",
        "price": 50,
        "telegram_gift_id": "5170314324215857265"
    },
    {
        "id": "gift_50_3",
        "title": "Шампанское",  # <-- поменяй
        "emoji": "🍾",
        "price": 50,
        "telegram_gift_id": "5170564780938756245"
    },
    {
        "id": "gift_50_4",
        "title": "Букет",  # <-- поменяй
        "emoji": "💐",
        "price": 50,
        "telegram_gift_id": "5893356958802511476"
    },
    {
        "id": "gift_50_5",
        "title": "Подарок",  # <-- поменяй (это новый, добавлен недавно)
        "emoji": "🎁",
        "price": 50,
        "telegram_gift_id": "6028601630662853006"
    },
    
    # 100 Stars (3 подарка)
    {
        "id": "gift_100_1",
        "title": "Медведь",  # <-- поменяй
        "emoji": "🧸",
        "price": 100,
        "telegram_gift_id": "5168043875654172773"
    },
    {
        "id": "gift_100_2",
        "title": "Кольцо",  # <-- поменяй
        "emoji": "💍",
        "price": 100,
        "telegram_gift_id": "5170521118301225164"
    },
    {
        "id": "gift_100_3",
        "title": "Корона",  # <-- поменяй
        "emoji": "👑",
        "price": 100,
        "telegram_gift_id": "5170690322832818290"
    },
]

# Подарок для pity-системы (гарант) - выбери из доступных
PITY_REWARD_GIFT = "gift_15_1"  # Самый дешёвый

# Словарь для быстрого доступа (заполняется при старте)
GIFTS = {}

# ===== КЕЙСЫ =====
CASES = {
    # Дешёвые кейсы с pity
    "mini": {"title": "🎲 Мини", "price": 1, "category": "cheap", "pity_enabled": True},
    "basic-5": {"title": "🎯 Базовый 5", "price": 5, "category": "cheap", "pity_enabled": True},
    "basic-10": {"title": "🎯 Базовый 10", "price": 10, "category": "cheap", "pity_enabled": True},
    "basic-15": {"title": "🎯 Базовый 15", "price": 15, "category": "cheap", "pity_enabled": True},
    
    # Кейсы с подарками (без pity, выше шанс)
    "premium": {"title": "💎 Премиум", "price": 50, "category": "gifts", "win_chance": 0.30},
    "rich": {"title": "💰 Богач", "price": 100, "category": "gifts", "win_chance": 0.35},
    
    # Star кейсы
    "star-100": {
        "title": "⭐ Star 100", 
        "price": 100, 
        "category": "stars", 
        "type": "stars", 
        "drops": [
            {"stars": 50, "chance": 0.70},
            {"stars": 100, "chance": 0.25},
            {"stars": 250, "chance": 0.05}
        ]
    },
    "star-500": {
        "title": "⭐ Star 500", 
        "price": 500, 
        "category": "stars", 
        "type": "stars",
        "drops": [
            {"stars": 250, "chance": 0.50},
            {"stars": 500, "chance": 0.40},
            {"stars": 1000, "chance": 0.10}
        ]
    },
}

# ===== КЭШ =====
CACHE = {
    "settings": {}, "settings_time": 0,
    "news": [], "news_time": 0,
    "sales": [], "sales_time": 0,
    "balances": {}, "pity": {},
    "promocodes": {}, "promocodes_time": 0,
}
CACHE_TTL = 30
CACHE_TTL_LONG = 120

MEMORY = {"pending_results": {}}

# ===== GOOGLE SHEETS =====
gs_client = None
spreadsheet = None


def init_google_sheets():
    global gs_client, spreadsheet
    if not GOOGLE_CREDENTIALS or not GOOGLE_SHEET_ID:
        print("⚠️ Google Sheets не настроен")
        return False
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        gs_client = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gs_client.open_by_key(GOOGLE_SHEET_ID)
        print("✅ Google Sheets подключён!")
        
        existing = [ws.title for ws in spreadsheet.worksheets()]
        sheets = {
            "promocodes": ["code", "reward_type", "reward_id", "max_uses", "uses", "used_by", "created", "paid"],
            "balances": ["user_id", "stars"],
            "pity": ["user_id", "spent"],
            "purchases": ["user_id", "type", "item_id", "paid", "sender", "timestamp"],
            "donations": ["user_id", "username", "amount", "timestamp"],
            "news": ["id", "title", "text", "image", "date", "active"],
            "sales": ["id", "title", "discount", "only_with_signature", "starts_at", "ends_at", "active"],
            "settings": ["key", "value"]
        }
        for name, headers in sheets.items():
            if name not in existing:
                ws = spreadsheet.add_worksheet(name, rows=1000, cols=10)
                ws.append_row(headers)
                if name == "settings":
                    ws.append_row(["maintenance", "FALSE"])
                    ws.append_row(["maintenance_text", "Идёт тех. перерыв."])
        
        _load_all_cache()
        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def _load_all_cache():
    try:
        _refresh_settings_cache()
        _refresh_news_cache()
        _refresh_sales_cache()
        _refresh_balances_cache()
        _refresh_pity_cache()
        print("✅ Кэш предзагружен!")
    except Exception as e:
        print(f"⚠️ Ошибка кэша: {e}")


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name) if spreadsheet else None
    except:
        return None


def _refresh_settings_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("settings")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["settings"] = {str(r.get("key", "")).strip().lower(): str(r.get("value", "")).strip() for r in rows if r.get("key")}
        CACHE["settings_time"] = time.time()
    except Exception as e:
        print(f"❌ settings: {e}")


def get_setting(key: str, default: str = "") -> str:
    if time.time() - CACHE.get("settings_time", 0) > CACHE_TTL:
        _refresh_settings_cache()
    return CACHE.get("settings", {}).get(key.lower(), default)


def is_maintenance_enabled() -> bool:
    return get_setting("maintenance", "FALSE").upper() in ("TRUE", "1", "YES")


def get_maintenance_text() -> str:
    return get_setting("maintenance_text", "Тех. перерыв")


def _refresh_news_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("news")
        if not ws: return
        rows = ws.get_all_records()
        news = []
        for r in rows:
            if str(r.get("active", "TRUE")).upper() == "FALSE": continue
            if not str(r.get("title", "")).strip(): continue
            news.append({
                "id": str(r.get("id", "")),
                "title": str(r.get("title", "")),
                "text": str(r.get("text", "")),
                "image": str(r.get("image", "")) or None,
                "date": str(r.get("date", ""))
            })
        news.sort(key=lambda x: x["date"], reverse=True)
        CACHE["news"] = news
        CACHE["news_time"] = time.time()
    except Exception as e:
        print(f"❌ news: {e}")


def get_news() -> list:
    if time.time() - CACHE.get("news_time", 0) > CACHE_TTL_LONG:
        _refresh_news_cache()
    return CACHE.get("news", [])


def _refresh_sales_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("sales")
        if not ws: return
        rows = ws.get_all_records()
        sales = []
        for r in rows:
            if str(r.get("active", "TRUE")).upper() == "FALSE": continue
            discount = int(r.get("discount", 0)) if str(r.get("discount", "")).isdigit() else 0
            if discount <= 0: continue
            sales.append({
                "id": str(r.get("id", "")),
                "title": str(r.get("title", "")) or "Скидка",
                "discount": discount,
                "only_with_signature": str(r.get("only_with_signature", "TRUE")).upper() in ("TRUE", "1", "YES")
            })
        CACHE["sales"] = sales
        CACHE["sales_time"] = time.time()
    except Exception as e:
        print(f"❌ sales: {e}")


def get_sales() -> list:
    if time.time() - CACHE.get("sales_time", 0) > CACHE_TTL_LONG:
        _refresh_sales_cache()
    return CACHE.get("sales", [])


def _refresh_balances_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("balances")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["balances"] = {str(r.get("user_id", "")): int(r.get("stars", 0)) for r in rows if r.get("user_id")}
    except Exception as e:
        print(f"❌ balances: {e}")


def _refresh_pity_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("pity")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["pity"] = {str(r.get("user_id", "")): int(r.get("spent", 0)) for r in rows if r.get("user_id")}
    except Exception as e:
        print(f"❌ pity: {e}")


def get_star_balance(user_id: int) -> int:
    return CACHE.get("balances", {}).get(str(user_id), 0)


def set_star_balance(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("balances", {})[uid] = amount
    if not spreadsheet: return
    try:
        ws = get_sheet("balances")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == uid:
                found = i + 1
                break
        if found:
            ws.update(f"B{found}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_balance: {e}")


def add_star_balance(user_id: int, amount: int):
    set_star_balance(user_id, get_star_balance(user_id) + amount)


def use_star_balance(user_id: int, amount: int) -> bool:
    cur = get_star_balance(user_id)
    if cur < amount: return False
    set_star_balance(user_id, cur - amount)
    return True


def get_pity_spent(user_id: int) -> int:
    return CACHE.get("pity", {}).get(str(user_id), 0)


def set_pity_spent(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("pity", {})[uid] = amount
    if not spreadsheet: return
    try:
        ws = get_sheet("pity")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == uid:
                found = i + 1
                break
        if found:
            ws.update(f"B{found}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_pity: {e}")


def add_pity_spent(user_id: int, amount: int) -> int:
    new = get_pity_spent(user_id) + amount
    set_pity_spent(user_id, new)
    return new


def reset_pity(user_id: int):
    set_pity_spent(user_id, 0)


def get_promocodes() -> dict:
    now = time.time()
    if now - CACHE.get("promocodes_time", 0) < CACHE_TTL and CACHE.get("promocodes"):
        return CACHE["promocodes"]
    if not spreadsheet: return {}
    try:
        ws = get_sheet("promocodes")
        if not ws: return {}
        rows = ws.get_all_records()
        result = {}
        for r in rows:
            code = str(r.get("code", "")).strip()
            if not code: continue
            ub = str(r.get("used_by", ""))
            try: used_by = json.loads(ub) if ub else []
            except: used_by = []
            result[code] = {
                "reward_type": str(r.get("reward_type", "")),
                "reward_id": str(r.get("reward_id", "")),
                "max_uses": int(r.get("max_uses", 0)),
                "uses": int(r.get("uses", 0)),
                "used_by": [int(x) for x in used_by if str(x).isdigit()],
                "created": str(r.get("created", ""))
            }
        CACHE["promocodes"] = result
        CACHE["promocodes_time"] = now
        return result
    except Exception as e:
        print(f"❌ promocodes: {e}")
        return {}


def save_promocode(code: str, promo: dict):
    CACHE.setdefault("promocodes", {})[code] = promo
    CACHE["promocodes_time"] = time.time()
    if not spreadsheet: return
    try:
        ws = get_sheet("promocodes")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == code:
                found = i + 1
                break
        data = [
            code,
            promo.get("reward_type", ""),
            promo.get("reward_id", ""),
            promo.get("max_uses", 0),
            promo.get("uses", 0),
            json.dumps(promo.get("used_by", [])),
            promo.get("created", ""),
            "TRUE"
        ]
        if found:
            ws.update(f"A{found}:H{found}", [data])
        else:
            ws.append_row(data)
    except Exception as e:
        print(f"❌ save_promo: {e}")


def delete_promocode(code: str):
    CACHE.get("promocodes", {}).pop(code, None)
    if not spreadsheet: return
    try:
        ws = get_sheet("promocodes")
        if not ws: return
        for i, row in enumerate(ws.get_all_values()):
            if i == 0: continue
            if row and row[0] == code:
                ws.delete_rows(i + 1)
                return
    except Exception as e:
        print(f"❌ del_promo: {e}")


def save_purchase(user_id: int, data: dict):
    if not spreadsheet: return
    try:
        ws = get_sheet("purchases")
        if ws:
            ws.append_row([
                str(user_id),
                data.get("type", ""),
                data.get("gift_id", data.get("case_id", "")),
                data.get("paid", 0),
                data.get("sender", ""),
                datetime.now().isoformat()
            ])
    except: pass


def save_donation(user_id: int, username: str, amount: int):
    if not spreadsheet: return
    try:
        ws = get_sheet("donations")
        if ws:
            ws.append_row([str(user_id), username or "", amount, datetime.now().isoformat()])
    except: pass


def save_pending_result(payment_id: str, result: dict):
    MEMORY["pending_results"][payment_id] = {**result, "ts": time.time()}


def get_pending_result(payment_id: str) -> Optional[dict]:
    return MEMORY.get("pending_results", {}).pop(payment_id, None)


def format_gift_text(sender_key: str, recipient_username: str = None) -> str:
    if not sender_key or sender_key not in SENDERS:
        return None
    r = recipient_username.lstrip("@") if recipient_username else None
    return f"От {sender_key} для @{r}" if r else f"От {sender_key}"


def calc_gift_price(gift_id: str, sender: Optional[str]) -> dict:
    gift = GIFTS.get(gift_id)
    if not gift:
        return {"final_price": 0, "base_price": 0, "signature_cost": 0, "discount": 0}
    base = gift["price"]
    sig = SIGNATURE_COSTS.get(sender, 0) if sender in SENDERS else 0
    sale = None
    if sender in SENDERS:
        for s in get_sales():
            if s.get("only_with_signature"):
                sale = s
                break
    disc = sale.get("discount", 0) if sale else 0
    return {
        "final_price": max(0, base - disc) + sig,
        "base_price": base,
        "signature_cost": sig,
        "discount": disc
    }


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


def validate_init_data(init_data: str) -> Optional[dict]:
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        if "hash" not in parsed:
            return None
        h = parsed.pop("hash")
        dcs = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest() == h:
            if "user" in parsed:
                parsed["user"] = json.loads(parsed["user"])
            return parsed
        return None
    except:
        return None


async def check_subscription(user_id: int) -> dict:
    if not REQUIRED_CHANNELS:
        return {"subscribed": True, "missing": []}
    missing = []
    for ch in REQUIRED_CHANNELS:
        ch = ch.strip()
        if not ch:
            continue
        try:
            cid = int(ch) if ch.lstrip('-').isdigit() else ch
            m = await bot.get_chat_member(chat_id=cid, user_id=user_id)
            st = m.status.value if hasattr(m.status, 'value') else str(m.status).lower()
            if st in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:
            if "user not found" in str(e).lower():
                missing.append(ch)
    return {"subscribed": len(missing) == 0, "missing": list(dict.fromkeys(missing))}


async def load_telegram_gifts():
    """Загружаем подарки из Telegram и сопоставляем по telegram_gift_id"""
    global GIFTS
    
    try:
        available = await bot.get_available_gifts()
        if not available or not available.gifts:
            print("❌ Нет подарков в Telegram API")
            # Помечаем все как недоступные
            for hg in HARDCODED_GIFTS:
                GIFTS[hg["id"]] = {
                    **hg,
                    "is_limited": False,
                    "sold_out": True,
                    "sticker_url": None,
                    "sticker_type": None,
                }
            return False
        
        print(f"📦 Telegram вернул: {len(available.gifts)} подарков")
        
        # Создаём словарь telegram подарков по id
        tg_gifts_map = {tg.id: tg for tg in available.gifts}
        
        # Множество использованных telegram_gift_id
        used_tg_ids = set()
        
        # 1. Сопоставляем захардкоженные подарки по telegram_gift_id
        for hg in HARDCODED_GIFTS:
            gid = hg["id"]
            tg_id = hg.get("telegram_gift_id")
            
            if tg_id and tg_id in tg_gifts_map:
                # Есть точное сопоставление!
                tg = tg_gifts_map[tg_id]
                used_tg_ids.add(tg_id)
                
                is_limited = tg.total_count is not None and tg.total_count > 0
                sold_out = is_limited and (tg.remaining_count or 0) == 0
                
                # Получаем данные стикера
                sticker_url, sticker_type = await get_sticker_data(tg.sticker)
                
                GIFTS[gid] = {
                    "id": gid,
                    "title": hg["title"],
                    "emoji": hg["emoji"],
                    "price": tg.star_count,  # Берём цену из API (может измениться)
                    "telegram_gift_id": tg_id,
                    "is_limited": is_limited,
                    "sold_out": sold_out,
                    "total_count": tg.total_count,
                    "remaining_count": tg.remaining_count,
                    "sticker_url": sticker_url,
                    "sticker_type": sticker_type,
                }
                print(f"  ✓ {hg['emoji']} {hg['title']} ({tg.star_count}⭐) -> {tg_id}")
            else:
                # Нет в API или telegram_gift_id не задан
                GIFTS[gid] = {
                    "id": gid,
                    "title": hg["title"],
                    "emoji": hg["emoji"],
                    "price": hg["price"],
                    "telegram_gift_id": None,
                    "is_limited": False,
                    "sold_out": True,
                    "sticker_url": None,
                    "sticker_type": None,
                }
                if tg_id:
                    print(f"  ✗ {hg['emoji']} {hg['title']} - id {tg_id} НЕ НАЙДЕН в API!")
                else:
                    print(f"  ⚠ {hg['emoji']} {hg['title']} - telegram_gift_id не задан")
        
        # 2. Добавляем новые подарки из Telegram (которых нет в HARDCODED_GIFTS)
        for tg in available.gifts:
            if tg.id in used_tg_ids:
                continue
            
            gid = f"tg_{tg.id}"
            
            is_limited = tg.total_count is not None and tg.total_count > 0
            sold_out = is_limited and (tg.remaining_count or 0) == 0
            
            sticker_url, sticker_type = await get_sticker_data(tg.sticker)
            
            GIFTS[gid] = {
                "id": gid,
                "title": f"Подарок {tg.star_count}⭐",  # Без названия
                "emoji": "🎁",
                "price": tg.star_count,
                "telegram_gift_id": tg.id,
                "is_limited": is_limited,
                "is_unknown": True,  # Пометка что это неизвестный
                "sold_out": sold_out,
                "total_count": tg.total_count,
                "remaining_count": tg.remaining_count,
                "sticker_url": sticker_url,
                "sticker_type": sticker_type,
            }
            print(f"  + Новый: {tg.id} ({tg.star_count}⭐)")
        
        available_count = sum(1 for g in GIFTS.values() if g.get("telegram_gift_id") and not g.get("sold_out"))
        print(f"✅ Итого: {len(GIFTS)} подарков, {available_count} доступно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        import traceback
        traceback.print_exc()
        return False


async def get_sticker_data(sticker) -> tuple[str | None, str | None]:
    """Получает URL и тип стикера"""
    try:
        file = await bot.get_file(sticker.file_id)
        url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        
        # Определяем тип по расширению
        if file.file_path:
            if file.file_path.endswith('.tgs'):
                return url, 'tgs'
            elif file.file_path.endswith('.webm'):
                return url, 'webm'
            elif file.file_path.endswith('.webp'):
                return url, 'webp'
        
        return url, 'unknown'
    except Exception as e:
        print(f"  ⚠ Ошибка стикера: {e}")
        return None, None


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    """Отправляет реальный подарок через Telegram"""
    gift = GIFTS.get(gift_id)
    if not gift:
        return False, "Подарок не найден"
    
    tg_id = gift.get("telegram_gift_id")
    if not tg_id:
        return False, "Подарок недоступен"
    if gift.get("sold_out"):
        return False, "Подарок закончился"
    
    try:
        await bot.send_gift(
            user_id=user_id,
            gift_id=tg_id,
            text=text or f"{gift['emoji']} {gift['title']}"
        )
        return True, "OK"
    except Exception as e:
        msg = str(e)
        if "DISALLOWED" in msg.upper():
            return False, "🔒 Включи получение подарков в настройках"
        if "GIFT_SOLD_OUT" in msg.upper():
            # Помечаем как проданный
            if gift_id in GIFTS:
                GIFTS[gift_id]["sold_out"] = True
            return False, "😔 Подарок закончился"
        return False, msg[:80]


def roll_case(case_id: str, user_id: int = None) -> dict:
    """Крутит кейс и возвращает результат"""
    case = CASES.get(case_id)
    if not case:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # Star кейсы
    if case.get("type") == "stars":
        drops = case.get("drops", [])
        roll = random.random()
        cum = 0
        for d in drops:
            cum += d["chance"]
            if roll < cum:
                return {"type": "stars", "stars_won": d["stars"], "all_drops": drops}
        return {"type": "stars", "stars_won": drops[0]["stars"] if drops else 50, "all_drops": drops}
    
    # Pity система для дешёвых кейсов
    if case.get("pity_enabled") and user_id:
        new_spent = add_pity_spent(user_id, case["price"])
        if new_spent >= PITY_THRESHOLD:
            reset_pity(user_id)
            return {
                "type": "gift",
                "items": [PITY_REWARD_GIFT],
                "multiplier": 1,
                "pity_triggered": True,
                "pity_progress": 0
            }
        # Для pity кейсов - всегда ничего (пока не накопится)
        return {"type": "nothing", "items": [], "multiplier": 1, "pity_progress": new_spent}
    
    # Обычные кейсы с шансом выигрыша
    win_chance = case.get("win_chance", 0.25)
    
    # Фильтруем доступные подарки по цене кейса
    available = [
        gid for gid, g in GIFTS.items()
        if g.get("telegram_gift_id")
        and not g.get("sold_out")
        and g["price"] <= case["price"]
    ]
    
    if not available:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # Проверяем выиграл ли
    if random.random() < win_chance:
        # Выбираем случайный подарок
        won_gift = random.choice(available)
        return {"type": "gift", "items": [won_gift], "multiplier": 1}
    
    return {"type": "nothing", "items": [], "multiplier": 1}


# ===== КОМАНДЫ БОТА =====

@router.message(Command("start"))
async def cmd_start(message: Message):
    uid = message.from_user.id
    if is_maintenance_enabled() and not is_admin(uid):
        await message.answer(get_maintenance_text())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer(
        "👋 <b>Подарочница</b>\n\n🎁 Подарки • 🎰 Кейсы • ⭐ Stars",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    p = message.successful_payment
    payload = json.loads(p.invoice_payload)
    uid = message.from_user.id
    uname = message.from_user.username
    total = p.total_amount
    itype = payload.get("type")
    pid = payload.get("payment_id")
    
    try:
        if itype == "donate":
            save_donation(uid, uname, total)
            await message.answer(f"💝 Спасибо за донат {total}⭐!")
            return
        
        if itype == "gift":
            gift = GIFTS.get(payload["id"])
            if not gift:
                await message.answer("⚠️ Подарок не найден")
                return
            txt = format_gift_text(payload.get("sender"), uname)
            ok, err = await send_real_gift(uid, payload["id"], txt)
            save_purchase(uid, {"type": "gift", "gift_id": payload["id"], "paid": total})
            await message.answer(f"🎉 {gift['emoji']} {gift['title']}!" if ok else f"⚠️ {err}")
            return
        
        if itype == "case":
            case = CASES.get(payload["id"])
            if not case:
                await message.answer("⚠️ Кейс не найден")
                return
            
            result = roll_case(payload["id"], uid)
            if pid:
                save_pending_result(pid, result)
            
            if result["type"] == "stars":
                add_star_balance(uid, result["stars_won"])
                await message.answer(f"⭐ +{result['stars_won']} Stars!")
            elif result["type"] == "nothing":
                pity = result.get("pity_progress", 0)
                msg = "😔 Ничего..."
                if pity:
                    msg += f"\n📊 До гарантии: {PITY_THRESHOLD - pity}⭐"
                await message.answer(msg)
            else:
                # Выиграли подарок(и)
                for gid in result["items"]:
                    await send_real_gift(uid, gid, f"🎰 Из кейса {case['title']}")
                    await asyncio.sleep(0.3)
                
                g = GIFTS.get(result["items"][0], {})
                msg = f"🎉 {g.get('emoji', '🎁')} {g.get('title', 'Подарок')}!"
                if result.get("pity_triggered"):
                    msg = f"🎯 ГАРАНТ! {msg}"
                await message.answer(msg)
            
            save_purchase(uid, {"type": "case", "case_id": payload["id"], "paid": total})
            
    except Exception as e:
        print(f"❌ Payment error: {e}")
        import traceback
        traceback.print_exc()
        await message.answer("⚠️ Произошла ошибка, напишите в поддержку")


async def keep_alive():
    if not SELF_URL:
        return
    await asyncio.sleep(30)
    async with httpx.AsyncClient() as c:
        while True:
            try:
                await c.get(f"{SELF_URL}/health", timeout=10)
            except:
                pass
            await asyncio.sleep(240)


async def refresh_gifts_loop():
    """Обновляем подарки каждые 10 минут"""
    await asyncio.sleep(60)
    while True:
        try:
            await load_telegram_gifts()
        except Exception as e:
            print(f"⚠️ refresh: {e}")
        await asyncio.sleep(600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()
    await asyncio.sleep(1)
    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    asyncio.create_task(refresh_gifts_loop())
    print("✅ Готово!")
    yield
    print("👋 Стоп")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# ===== PYDANTIC MODELS =====

class InitDataReq(BaseModel):
    initData: str

class InvoiceReq(BaseModel):
    initData: str
    giftId: str | None = None
    caseId: str | None = None
    sender: str | None = None

class PromocodeReq(BaseModel):
    initData: str
    code: str

class CreatePromocodeReq(BaseModel):
    initData: str
    code: str
    rewardType: str
    rewardId: str
    maxUses: int

class DeletePromocodeReq(BaseModel):
    initData: str
    code: str

class DonateReq(BaseModel):
    initData: str
    amount: int

class BuyWithBalanceReq(BaseModel):
    initData: str
    giftId: str | None = None
    caseId: str | None = None
    sender: str | None = None

class GetResultReq(BaseModel):
    initData: str
    paymentId: str


def raise_if_maintenance(uid: int):
    if is_maintenance_enabled() and not is_admin(uid):
        raise HTTPException(503, get_maintenance_text())


async def require_subscription(uid: int):
    sub = await check_subscription(uid)
    if not sub["subscribed"]:
        raise HTTPException(403, "Подпишитесь на каналы")


# ===== API ENDPOINTS =====

@app.post("/api/check-subscription")
async def api_check_subscription(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    
    if is_maintenance_enabled() and not is_admin(uid):
        return {"subscribed": False, "maintenance": True, "message": get_maintenance_text(), "channels": []}
    
    result = await check_subscription(uid)
    channels = []
    for ch in REQUIRED_CHANNELS:
        link = CHANNEL_LINKS.get(ch)
        title = ch
        try:
            cid = int(ch) if ch.lstrip('-').isdigit() else ch
            chat = await bot.get_chat(cid)
            title = chat.title or ch
            if not link:
                if chat.username:
                    link = f"https://t.me/{chat.username}"
                elif chat.invite_link:
                    link = chat.invite_link
        except:
            pass
        channels.append({
            "id": ch,
            "title": title,
            "link": link,
            "missing": ch in result["missing"]
        })
    
    return {"subscribed": result["subscribed"], "maintenance": False, "channels": channels}


@app.post("/api/get-user-data")
async def api_get_user_data(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    return {
        "starBalance": get_star_balance(uid),
        "pitySpent": get_pity_spent(uid),
        "pityThreshold": PITY_THRESHOLD,
        "isAdmin": is_admin(uid)
    }


@app.get("/api/sticker/{gift_id}")
async def get_sticker_proxy(gift_id: str):
    """Проксирует TGS стикер и отдаёт распакованный Lottie JSON"""
    gift = GIFTS.get(gift_id)
    if not gift or not gift.get("sticker_url"):
        raise HTTPException(404, "Not found")
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(gift["sticker_url"], timeout=10)
            if resp.status_code != 200:
                raise HTTPException(502, "Failed to fetch sticker")
            
            data = resp.content
            
            # TGS = gzipped Lottie JSON, распаковываем
            if gift.get("sticker_type") == "tgs":
                import gzip
                try:
                    data = gzip.decompress(data)
                except:
                    pass  # Может уже распакован
                
                from fastapi.responses import Response
                return Response(
                    content=data,
                    media_type="application/json",
                    headers={
                        "Access-Control-Allow-Origin": "*",
                        "Cache-Control": "public, max-age=86400",
                    }
                )
            
            # Для других типов отдаём как есть
            from fastapi.responses import Response
            content_type = "image/webp" if gift.get("sticker_type") == "webp" else "video/webm"
            return Response(
                content=data,
                media_type=content_type,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "public, max-age=86400",
                }
            )
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Sticker proxy error: {e}")
        raise HTTPException(502, "Proxy error")

@app.get("/api/get-gifts")
async def api_get_gifts():
    gifts = []
    for gid, g in GIFTS.items():
        gifts.append({
            "id": gid,
            "title": g.get("title"),
            "emoji": g.get("emoji", "🎁"),
            "price": g["price"],
            "sticker_url": g.get("sticker_url"),
            "sticker_type": g.get("sticker_type"),
            "is_limited": g.get("is_limited", False),
            "is_unknown": g.get("is_unknown", False),
            "remaining": g.get("remaining_count"),
            "total": g.get("total_count"),
            "sold_out": g.get("sold_out", False),
            "available": g.get("telegram_gift_id") is not None and not g.get("sold_out", False),
        })
    
    # Сортировка: доступные первые, потом по цене
    gifts.sort(key=lambda x: (not x["available"], x["sold_out"], x["price"], x["id"]))
    return {"gifts": gifts, "sales": get_sales()}


@app.get("/api/get-cases")
async def api_get_cases():
    cats = {
        "cheap": {"title": "💰 Дешёвые", "cases": []},
        "gifts": {"title": "🎁 Подарки", "cases": []},
        "stars": {"title": "⭐ Stars", "cases": []}
    }
    for cid, c in CASES.items():
        cat = c.get("category", "gifts")
        if cat in cats:
            cats[cat]["cases"].append({
                "id": cid,
                "title": c["title"],
                "price": c["price"],
                "category": cat,
                "type": c.get("type"),
                "pity_enabled": c.get("pity_enabled", False)
            })
    return {"categories": cats, "pityThreshold": PITY_THRESHOLD}


@app.get("/api/get-news")
async def api_get_news():
    return {"news": get_news()}


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    pid = str(uuid.uuid4())
    
    if req.giftId and req.giftId in GIFTS:
        g = GIFTS[req.giftId]
        if g.get("sold_out") or not g.get("telegram_gift_id"):
            raise HTTPException(400, "Подарок недоступен")
        
        price_info = calc_gift_price(req.giftId, req.sender)
        price = price_info["final_price"]
        
        link = await bot.create_invoice_link(
            title=f"{g['emoji']} {g['title']}",
            description=f"Подарок {g['title']}",
            payload=json.dumps({
                "type": "gift",
                "id": req.giftId,
                "sender": req.sender,
                "payment_id": pid
            }),
            currency="XTR",
            prices=[LabeledPrice(label=g["title"], amount=price)]
        )
        return {"link": link, "paymentId": pid}
    
    if req.caseId and req.caseId in CASES:
        c = CASES[req.caseId]
        link = await bot.create_invoice_link(
            title=c["title"],
            description="🎰 Испытай удачу!",
            payload=json.dumps({
                "type": "case",
                "id": req.caseId,
                "payment_id": pid
            }),
            currency="XTR",
            prices=[LabeledPrice(label=c["title"], amount=c["price"])]
        )
        return {"link": link, "paymentId": pid}
    
    raise HTTPException(400, "Товар не найден")


@app.post("/api/get-case-result")
async def api_get_case_result(req: GetResultReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    result = get_pending_result(req.paymentId)
    if not result:
        raise HTTPException(404, "Результат не найден")
    
    uid = auth["user"]["id"]
    result["newBalance"] = get_star_balance(uid)
    result["pitySpent"] = get_pity_spent(uid)
    
    # Добавляем инфу о выигранных подарках
    if result.get("items"):
        result["won"] = []
        for gid in result["items"]:
            g = GIFTS.get(gid, {})
            result["won"].append({
                "id": gid,
                "title": g.get("title"),
                "emoji": g.get("emoji", "🎁"),
                "price": g.get("price", 0),
                "sticker_url": g.get("sticker_url"),
                "sticker_type": g.get("sticker_type"),
            })
    
    return result


@app.post("/api/buy-with-balance")
async def api_buy_with_balance(req: BuyWithBalanceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    uname = auth["user"].get("username", "")
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    bal = get_star_balance(uid)
    
    # Покупка подарка
    if req.giftId and req.giftId in GIFTS:
        g = GIFTS[req.giftId]
        if g.get("sold_out") or not g.get("telegram_gift_id"):
            raise HTTPException(400, "Подарок недоступен")
        
        price = calc_gift_price(req.giftId, req.sender)["final_price"]
        if bal < price:
            raise HTTPException(400, f"Нужно {price}⭐, у вас {bal}⭐")
        
        use_star_balance(uid, price)
        ok, err = await send_real_gift(uid, req.giftId, format_gift_text(req.sender, uname))
        save_purchase(uid, {"type": "gift_balance", "gift_id": req.giftId, "paid": price})
        
        if not ok:
            add_star_balance(uid, price)  # Возврат
            raise HTTPException(500, err)
        
        return {
            "success": True,
            "type": "gift",
            "reward": f"{g['emoji']} {g['title']}",
            "newBalance": get_star_balance(uid)
        }
    
    # Покупка кейса
    if req.caseId and req.caseId in CASES:
        c = CASES[req.caseId]
        price = c["price"]
        if bal < price:
            raise HTTPException(400, f"Нужно {price}⭐, у вас {bal}⭐")
        
        use_star_balance(uid, price)
        result = roll_case(req.caseId, uid)
        
        if result["type"] == "stars":
            add_star_balance(uid, result["stars_won"])
            return {
                "success": True,
                "type": "stars",
                "starsWon": result["stars_won"],
                "newBalance": get_star_balance(uid),
                "allDrops": result.get("all_drops", [])
            }
        
        if result["type"] == "nothing":
            return {
                "success": True,
                "type": "nothing",
                "newBalance": get_star_balance(uid),
                "pitySpent": result.get("pity_progress", 0)
            }
        
        # Выиграли подарки
        sent = []
        for gid in result["items"]:
            g = GIFTS.get(gid, {})
            ok, _ = await send_real_gift(uid, gid, f"🎰 Из кейса {c['title']}")
            if ok:
                sent.append({
                    "id": gid,
                    "title": g.get("title"),
                    "emoji": g.get("emoji", "🎁"),
                    "price": g.get("price", 0),
                    "sticker_url": g.get("sticker_url"),
                    "sticker_type": g.get("sticker_type"),
                })
            await asyncio.sleep(0.3)
        
        return {
            "success": True,
            "type": "gifts",
            "won": sent,
            "multiplier": result.get("multiplier", 1),
            "newBalance": get_star_balance(uid),
            "pityTriggered": result.get("pity_triggered", False),
            "pitySpent": get_pity_spent(uid)
        }
    
    raise HTTPException(400, "Не указан товар")


@app.post("/api/create-donate")
async def create_donate(req: DonateReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    if req.amount < 1 or req.amount > 10000:
        raise HTTPException(400, "Сумма от 1 до 10000")
    
    link = await bot.create_invoice_link(
        title="💝 Донат",
        description=f"Поддержка проекта на {req.amount}⭐",
        payload=json.dumps({"type": "donate", "amount": req.amount}),
        currency="XTR",
        prices=[LabeledPrice(label="Донат", amount=req.amount)]
    )
    return {"link": link}


@app.post("/api/activate-promocode")
async def api_activate_promocode(req: PromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = int(auth["user"]["id"])
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    code = req.code.strip().upper()
    promos = get_promocodes()
    
    if code not in promos:
        raise HTTPException(404, "Промокод не найден")
    
    p = promos[code]
    if uid in p.get("used_by", []):
        raise HTTPException(400, "Вы уже использовали этот промокод")
    if p.get("uses", 0) >= p.get("max_uses", 0):
        raise HTTPException(400, "Промокод закончился")
    
    # Используем
    p["uses"] = p.get("uses", 0) + 1
    p.setdefault("used_by", []).append(uid)
    save_promocode(code, p)
    
    rt, ri = p["reward_type"], p["reward_id"]
    
    if rt == "stars":
        amt = int(ri)
        add_star_balance(uid, amt)
        return {"success": True, "reward": f"+{amt}⭐", "newBalance": get_star_balance(uid)}
    
    if rt == "gift":
        g = GIFTS.get(ri)
        if g and g.get("telegram_gift_id"):
            ok, err = await send_real_gift(uid, ri, f"🎟 Промокод {code}")
            if ok:
                return {"success": True, "reward": f"{g['emoji']} {g['title']}"}
            return {"success": False, "error": err}
    
    if rt == "case":
        result = roll_case(ri, uid)
        return {"success": True, "reward": "Кейс открыт!", "caseResult": result}
    
    return {"success": True, "reward": "OK"}


# ===== ADMIN ENDPOINTS =====

@app.post("/api/admin/get-promocodes")
async def api_admin_get_promocodes(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    promos = get_promocodes()
    return {
        "promocodes": [
            {
                "code": c,
                "rewardType": p["reward_type"],
                "rewardId": p["reward_id"],
                "maxUses": p["max_uses"],
                "uses": p["uses"],
                "created": p.get("created", "")
            }
            for c, p in promos.items()
        ]
    }


@app.post("/api/admin/create-promocode")
async def api_admin_create_promocode(req: CreatePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    code = req.code.strip().upper()
    if not code:
        raise HTTPException(400, "Код не указан")
    
    save_promocode(code, {
        "reward_type": req.rewardType,
        "reward_id": req.rewardId,
        "max_uses": req.maxUses,
        "uses": 0,
        "used_by": [],
        "created": datetime.now().isoformat()
    })
    return {"success": True, "code": code}


@app.post("/api/admin/delete-promocode")
async def api_admin_delete_promocode(req: DeletePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    delete_promocode(req.code.upper())
    return {"success": True}


@app.get("/api/admin/get-gifts-list")
async def api_admin_get_gifts_list():
    """Список подарков для админки (создание промокодов)"""
    gifts_list = []
    for gid, g in sorted(GIFTS.items(), key=lambda x: (x[1]["price"], x[0])):
        if not g.get("telegram_gift_id"):
            continue  # Пропускаем недоступные
        gifts_list.append({
            "id": gid,
            "title": f"{g['emoji']} {g['title']} ({g['price']}⭐)",
            "price": g["price"],
        })
    
    cases_list = [
        {"id": cid, "title": c["title"], "price": c["price"]}
        for cid, c in CASES.items()
    ]
    
    return {"gifts": gifts_list, "cases": cases_list}


@app.get("/api/debug/list-telegram-gifts")
async def debug_list_gifts():
    """Debug: показать все подарки из Telegram API"""
    try:
        available = await bot.get_available_gifts()
        gifts = []
        for tg in available.gifts:
            sticker_url, sticker_type = await get_sticker_data(tg.sticker)
            gifts.append({
                "telegram_gift_id": tg.id,
                "price": tg.star_count,
                "sticker_url": sticker_url,
                "sticker_type": sticker_type,
                "is_limited": tg.total_count is not None,
                "total_count": tg.total_count,
                "remaining_count": tg.remaining_count,
            })
        
        gifts.sort(key=lambda x: (x["price"], x["telegram_gift_id"]))
        return {"count": len(gifts), "gifts": gifts}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/refresh-gifts")
async def refresh_gifts():
    """Принудительно обновить подарки"""
    ok = await load_telegram_gifts()
    return {
        "success": ok,
        "gifts_count": len(GIFTS),
        "available": sum(1 for g in GIFTS.values() if g.get("telegram_gift_id") and not g.get("sold_out"))
    }


@app.get("/")
async def root():
    available = sum(1 for g in GIFTS.values() if g.get("telegram_gift_id") and not g.get("sold_out"))
    return {"app": "Подарочница v11.0", "gifts": len(GIFTS), "available": available}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

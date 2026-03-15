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
print("🚀 ПОДАРОЧНИЦА v7.2 — ОПТИМИЗИРОВАННАЯ")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
# ===== ПАРСИНГ КАНАЛОВ =====
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

print(f"📋 REQUIRED_CHANNELS: {REQUIRED_CHANNELS}")
print(f"📋 CHANNEL_LINKS: {CHANNEL_LINKS}")

SENDERS = ["@echoaxxs", "@bogclm", "@bogclm и @echoaxxs"]
SIGNATURE_COSTS = {"@echoaxxs": 2, "@bogclm": 2, "@bogclm и @echoaxxs": 5}

PITY_THRESHOLD = 20
PITY_REWARD_GIFT = "heart"

# ===== ПОДАРКИ =====
GIFTS = {
    "brilliant_ring": {"title": "💍 Колечко", "price": 100, "star_cost": 100, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/brilliant_ring.gif"},
    "heroic_cup": {"title": "🏆 Кубок", "price": 100, "star_cost": 100, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/heroic_cup.gif"},
    "diamond": {"title": "💎 Алмаз", "price": 100, "star_cost": 100, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/diamond.gif"},
    "flowers_bouquet": {"title": "💐 Букет", "price": 50, "star_cost": 50, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/bouquet_flowers.gif"},
    "cupcake": {"title": "🧁 Тортик", "price": 50, "star_cost": 50, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/cupcake.gif"},
    "wine": {"title": "🍷 Вино", "price": 50, "star_cost": 50, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/wine.gif"},
    "rocket": {"title": "🚀 Ракета", "price": 50, "star_cost": 50, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/rocket.gif"},
    "rose": {"title": "🌹 Роза", "price": 25, "star_cost": 25, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/rose.gif"},
    "box": {"title": "🎁 Подарок", "price": 25, "star_cost": 25, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/gift.gif"},
    "heart": {"title": "❤️ Сердце", "price": 15, "star_cost": 15, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/heart.gif"},
    "bear": {"title": "🧸 Мишка", "price": 15, "star_cost": 15, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/bear.gif"},
}

# ===== КЕЙСЫ =====
CASES = {
    "mini": {"title": "🎲 Мини", "price": 1, "category": "cheap", "drops": [{"gift_id": "heart", "chance": 0.0}, {"gift_id": "bear", "chance": 0.0}], "pity_enabled": True},
    "basic-5": {"title": "🎯 Базовый 5", "price": 5, "category": "cheap", "drops": [{"gift_id": "heart", "chance": 0.0}, {"gift_id": "bear", "chance": 0.0}], "pity_enabled": True},
    "basic-10": {"title": "🎯 Базовый 10", "price": 10, "category": "cheap", "drops": [{"gift_id": "heart", "chance": 0.0}, {"gift_id": "bear", "chance": 0.0}], "pity_enabled": True},
    "basic-15": {"title": "🎯 Базовый 15", "price": 15, "category": "cheap", "drops": [{"gift_id": "heart", "chance": 0.0}, {"gift_id": "bear", "chance": 0.0}], "pity_enabled": True},
    "premium": {"title": "💎 Премиум", "price": 50, "category": "gifts", "drops": [{"gift_id": "rose", "chance": 0.35}, {"gift_id": "box", "chance": 0.35}, {"gift_id": "nothing", "chance": 0.30}]},
    "rich": {"title": "💰 Богач", "price": 100, "category": "gifts", "drops": [{"gift_id": "rose", "chance": 0.30}, {"gift_id": "box", "chance": 0.30}, {"gift_id": "brilliant_ring", "chance": 0.10}, {"gift_id": "rocket", "chance": 0.15}, {"gift_id": "nothing", "chance": 0.15}]},
    "ultra": {"title": "🔥 Ультра", "price": 500, "category": "gifts", "drops": [{"gift_id": "brilliant_ring", "chance": 0.35}, {"gift_id": "diamond", "chance": 0.30}, {"gift_id": "heroic_cup", "chance": 0.30}, {"gift_id": "nothing", "chance": 0.05}], "multiplier": {"enabled": True, "chances": [{"count": 1, "chance": 0.50}, {"count": 2, "chance": 0.35}, {"count": 3, "chance": 0.15}]}},
    "star-100": {"title": "⭐ Star 100", "price": 100, "category": "stars", "type": "stars", "drops": [{"stars": 50, "chance": 0.70, "can_win": True}, {"stars": 100, "chance": 0.30, "can_win": True}, {"stars": 250, "chance": 0.50, "can_win": False}, {"stars": 500, "chance": 0.40, "can_win": False}, {"stars": 1000, "chance": 0.02, "can_win": False}]},
    "star-500": {"title": "⭐ Star 500", "price": 500, "category": "stars", "type": "stars", "drops": [{"stars": 250, "chance": 0.50, "can_win": True}, {"stars": 500, "chance": 0.40, "can_win": True}, {"stars": 1000, "chance": 0.08, "can_win": False}, {"stars": 1500, "chance": 0.015, "can_win": False}]},
    "star-1000": {"title": "⭐ Star 1000", "price": 1000, "category": "stars", "type": "stars", "drops": [{"stars": 500, "chance": 0.40, "can_win": True}, {"stars": 800, "chance": 0.30, "can_win": True}, {"stars": 1000, "chance": 0.20, "can_win": True}, {"stars": 1500, "chance": 0.07, "can_win": False}]},
}

# ===== КЭШ (главное для скорости!) =====
CACHE = {
    "settings": {},
    "settings_time": 0,
    "news": [],
    "news_time": 0,
    "sales": [],
    "sales_time": 0,
    "balances": {},
    "pity": {},
    "promocodes": {},
    "promocodes_time": 0,
}
CACHE_TTL = 30  # 30 секунд для настроек
CACHE_TTL_LONG = 120  # 2 минуты для новостей/акций

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
                    ws.append_row(["maintenance_text", "Идёт тех. перерыв, мы улучшаем подарочницу. Попробуй позже."])
        
        # Предзагрузка кэша
        _load_all_cache()
        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def _load_all_cache():
    """Предзагрузка всех данных в кэш при старте"""
    try:
        _refresh_settings_cache()
        _refresh_news_cache()
        _refresh_sales_cache()
        _refresh_balances_cache()
        _refresh_pity_cache()
        print("✅ Кэш предзагружен!")
    except Exception as e:
        print(f"⚠️ Ошибка предзагрузки кэша: {e}")


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name) if spreadsheet else None
    except:
        return None


# ===== КЭШИРОВАННЫЕ ФУНКЦИИ =====

def _refresh_settings_cache():
    """Обновить кэш настроек"""
    if not spreadsheet:
        return
    try:
        ws = get_sheet("settings")
        if not ws:
            return
        rows = ws.get_all_records()
        settings = {}
        for row in rows:
            k = str(row.get("key", "")).strip().lower()
            v = str(row.get("value", "")).strip()
            if k:
                settings[k] = v
        CACHE["settings"] = settings
        CACHE["settings_time"] = time.time()
    except Exception as e:
        print(f"❌ Ошибка загрузки settings: {e}")


def get_setting(key: str, default: str = "") -> str:
    """Получить настройку (с кэшем)"""
    now = time.time()
    if now - CACHE.get("settings_time", 0) > CACHE_TTL:
        _refresh_settings_cache()
    return CACHE.get("settings", {}).get(key.lower(), default)


def is_maintenance_enabled() -> bool:
    """Проверить тех. перерыв"""
    value = get_setting("maintenance", "FALSE")
    return value.upper() in ("TRUE", "1", "YES", "ON", "ДА")


def get_maintenance_text() -> str:
    """Получить текст тех. перерыва"""
    return get_setting("maintenance_text", "Идёт тех. перерыв, мы улучшаем подарочницу. Попробуй позже.")


def _refresh_news_cache():
    """Обновить кэш новостей"""
    if not spreadsheet:
        return
    try:
        ws = get_sheet("news")
        if not ws:
            return
        rows = ws.get_all_records()
        news = []
        for row in rows:
            if str(row.get("active", "TRUE")).strip().upper() == "FALSE":
                continue
            if not str(row.get("title", "")).strip():
                continue
            news.append({
                "id": str(row.get("id", "")),
                "title": str(row.get("title", "")),
                "text": str(row.get("text", "")),
                "image": str(row.get("image", "")) or None,
                "date": str(row.get("date", ""))
            })
        news.sort(key=lambda x: x["date"], reverse=True)
        CACHE["news"] = news
        CACHE["news_time"] = time.time()
    except Exception as e:
        print(f"❌ Ошибка загрузки news: {e}")


def get_news() -> list:
    """Получить новости (с кэшем)"""
    now = time.time()
    if now - CACHE.get("news_time", 0) > CACHE_TTL_LONG:
        _refresh_news_cache()
    return CACHE.get("news", [])


def _refresh_sales_cache():
    """Обновить кэш акций"""
    if not spreadsheet:
        return
    try:
        ws = get_sheet("sales")
        if not ws:
            return
        rows = ws.get_all_records()
        now_dt = datetime.now()
        sales = []
        for row in rows:
            if str(row.get("active", "TRUE")).strip().upper() == "FALSE":
                continue
            try:
                discount = int(row.get("discount", 0))
            except:
                discount = 0
            if discount <= 0:
                continue
            sales.append({
                "id": str(row.get("id", "")).strip(),
                "title": str(row.get("title", "")).strip() or "Скидка",
                "discount": discount,
                "only_with_signature": str(row.get("only_with_signature", "TRUE")).strip().upper() in ("TRUE", "1", "YES"),
            })
        CACHE["sales"] = sales
        CACHE["sales_time"] = time.time()
    except Exception as e:
        print(f"❌ Ошибка загрузки sales: {e}")


def get_sales() -> list:
    """Получить акции (с кэшем)"""
    now = time.time()
    if now - CACHE.get("sales_time", 0) > CACHE_TTL_LONG:
        _refresh_sales_cache()
    return CACHE.get("sales", [])


def _refresh_balances_cache():
    """Обновить кэш балансов"""
    if not spreadsheet:
        return
    try:
        ws = get_sheet("balances")
        if not ws:
            return
        rows = ws.get_all_records()
        balances = {}
        for row in rows:
            uid = str(row.get("user_id", ""))
            if uid:
                balances[uid] = int(row.get("stars", 0))
        CACHE["balances"] = balances
    except Exception as e:
        print(f"❌ Ошибка загрузки balances: {e}")


def _refresh_pity_cache():
    """Обновить кэш pity"""
    if not spreadsheet:
        return
    try:
        ws = get_sheet("pity")
        if not ws:
            return
        rows = ws.get_all_records()
        pity = {}
        for row in rows:
            uid = str(row.get("user_id", ""))
            if uid:
                pity[uid] = int(row.get("spent", 0))
        CACHE["pity"] = pity
    except Exception as e:
        print(f"❌ Ошибка загрузки pity: {e}")


# ===== БАЛАНС (с кэшем в памяти) =====

def get_star_balance(user_id: int) -> int:
    return CACHE.get("balances", {}).get(str(user_id), 0)


def set_star_balance(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("balances", {})[uid] = amount
    
    if not spreadsheet:
        return
    try:
        ws = get_sheet("balances")
        if not ws:
            return
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == uid:
                found_row = i + 1
                break
        if found_row:
            ws.update(f"B{found_row}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_star_balance: {e}")


def add_star_balance(user_id: int, amount: int):
    current = get_star_balance(user_id)
    set_star_balance(user_id, current + amount)


def use_star_balance(user_id: int, amount: int) -> bool:
    current = get_star_balance(user_id)
    if current < amount:
        return False
    set_star_balance(user_id, current - amount)
    return True


# ===== PITY (с кэшем в памяти) =====

def get_pity_spent(user_id: int) -> int:
    return CACHE.get("pity", {}).get(str(user_id), 0)


def set_pity_spent(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("pity", {})[uid] = amount
    
    if not spreadsheet:
        return
    try:
        ws = get_sheet("pity")
        if not ws:
            return
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == uid:
                found_row = i + 1
                break
        if found_row:
            ws.update(f"B{found_row}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_pity_spent: {e}")


def add_pity_spent(user_id: int, amount: int) -> int:
    current = get_pity_spent(user_id)
    new_total = current + amount
    set_pity_spent(user_id, new_total)
    return new_total


def reset_pity(user_id: int):
    set_pity_spent(user_id, 0)


# ===== ПРОМОКОДЫ =====

def get_promocodes() -> dict:
    now = time.time()
    if now - CACHE.get("promocodes_time", 0) < CACHE_TTL:
        cached = CACHE.get("promocodes", {})
        if cached:
            return cached
    
    if not spreadsheet:
        return {}
    try:
        ws = get_sheet("promocodes")
        if not ws:
            return {}
        rows = ws.get_all_records()
        result = {}
        for row in rows:
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            used_by_str = str(row.get("used_by", ""))
            try:
                used_by = json.loads(used_by_str) if used_by_str else []
            except:
                used_by = []
            result[code] = {
                "reward_type": str(row.get("reward_type", "")),
                "reward_id": str(row.get("reward_id", "")),
                "max_uses": int(row.get("max_uses", 0)),
                "uses": int(row.get("uses", 0)),
                "used_by": [int(x) for x in used_by if str(x).isdigit()],
                "created": str(row.get("created", "")),
            }
        CACHE["promocodes"] = result
        CACHE["promocodes_time"] = now
        return result
    except Exception as e:
        print(f"❌ get_promocodes: {e}")
        return {}


def save_promocode(code: str, promo: dict):
    CACHE.setdefault("promocodes", {})[code] = promo
    CACHE["promocodes_time"] = time.time()
    
    if not spreadsheet:
        return
    try:
        ws = get_sheet("promocodes")
        if not ws:
            return
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == code:
                found_row = i + 1
                break
        row_data = [
            code, promo.get("reward_type", ""), promo.get("reward_id", ""),
            promo.get("max_uses", 0), promo.get("uses", 0),
            json.dumps(promo.get("used_by", [])), promo.get("created", ""), "TRUE"
        ]
        if found_row:
            ws.update(f"A{found_row}:H{found_row}", [row_data])
        else:
            ws.append_row(row_data)
    except Exception as e:
        print(f"❌ save_promocode: {e}")


def delete_promocode(code: str):
    CACHE.get("promocodes", {}).pop(code, None)
    if not spreadsheet:
        return
    try:
        ws = get_sheet("promocodes")
        if not ws:
            return
        all_values = ws.get_all_values()
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == code:
                ws.delete_rows(i + 1)
                return
    except Exception as e:
        print(f"❌ delete_promocode: {e}")


# ===== СОХРАНЕНИЕ (асинхронное, без блокировки) =====

def save_purchase(user_id: int, data: dict):
    if not spreadsheet:
        return
    try:
        ws = get_sheet("purchases")
        if ws:
            ws.append_row([
                str(user_id), data.get("type", ""),
                data.get("gift_id", data.get("case_id", "")),
                data.get("paid", 0), data.get("sender", ""),
                datetime.now().isoformat()
            ])
    except:
        pass


def save_donation(user_id: int, username: str, amount: int):
    if not spreadsheet:
        return
    try:
        ws = get_sheet("donations")
        if ws:
            ws.append_row([str(user_id), username or "", amount, datetime.now().isoformat()])
    except:
        pass


def save_pending_result(payment_id: str, result: dict):
    MEMORY["pending_results"][payment_id] = {**result, "ts": time.time()}


def get_pending_result(payment_id: str) -> Optional[dict]:
    return MEMORY.get("pending_results", {}).pop(payment_id, None)


# ===== УТИЛИТЫ =====

def format_gift_text(sender_key: str, recipient_username: str = None) -> str:
    if not sender_key or sender_key not in SENDERS:
        return None
    recipient = recipient_username.lstrip("@") if recipient_username else None
    return f"От {sender_key} для @{recipient}" if recipient else f"От {sender_key}"


def get_gift_signature_sale(sender: Optional[str]) -> dict | None:
    if not sender or sender not in SENDERS:
        return None
    for sale in get_sales():
        if sale.get("only_with_signature", True):
            return sale
    return None


def calc_gift_price(gift_id: str, sender: Optional[str]) -> dict:
    gift = GIFTS.get(gift_id)
    if not gift:
        return {"final_price": 0, "base_price": 0, "signature_cost": 0, "discount": 0}
    base = gift["price"]
    sig_cost = SIGNATURE_COSTS.get(sender, 0) if sender in SENDERS else 0
    sale = get_gift_signature_sale(sender)
    discount = sale.get("discount", 0) if sale else 0
    final = max(0, base - discount) + sig_cost
    return {"final_price": final, "base_price": base, "signature_cost": sig_cost, "discount": discount}


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
        received_hash = parsed.pop("hash")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash == received_hash:
            if "user" in parsed:
                parsed["user"] = json.loads(parsed["user"])
            return parsed
        return None
    except:
        return None


async def check_subscription(user_id: int) -> dict:
    if not REQUIRED_CHANNELS:
        return {"subscribed": True, "missing": [], "checked": []}

    missing = []
    checked = []

    for channel in REQUIRED_CHANNELS:
        channel = channel.strip()
        if not channel:
            continue
        try:
            chat_id = int(channel) if channel.lstrip('-').isdigit() else channel
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            status = member.status.value if hasattr(member.status, 'value') else str(member.status).lower()
            checked.append({"channel": channel, "status": status})
            if status in ("left", "kicked"):
                missing.append(channel)
        except Exception as e:
            err = str(e).lower()
            checked.append({"channel": channel, "status": "error", "error": str(e)[:50]})
            if "user not found" in err:
                missing.append(channel)
            elif "chat not found" not in err and "bot is not a member" not in err:
                missing.append(channel)

    return {"subscribed": len(missing) == 0, "missing": list(dict.fromkeys(missing)), "checked": checked}


async def load_telegram_gifts():
    try:
        gifts = await bot.get_available_gifts()
        if not gifts or not gifts.gifts:
            return False
        
        gifts_by_price = {}
        for gift in gifts.gifts:
            gifts_by_price.setdefault(gift.star_count, []).append(gift)
        
        used = {}
        for gid, gdata in GIFTS.items():
            cost = gdata["star_cost"]
            if cost in gifts_by_price:
                idx = used.get(cost, 0)
                GIFTS[gid]["telegram_gift_id"] = gifts_by_price[cost][idx % len(gifts_by_price[cost])].id
                used[cost] = idx + 1
            elif gifts_by_price:
                closest = min(gifts_by_price.keys(), key=lambda x: abs(x - cost))
                idx = used.get(closest, 0)
                GIFTS[gid]["telegram_gift_id"] = gifts_by_price[closest][idx % len(gifts_by_price[closest])].id
                GIFTS[gid]["star_cost"] = closest
                used[closest] = idx + 1
        
        print("✅ Подарки загружены!")
        return True
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        return False


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    gift = GIFTS.get(gift_id)
    if not gift or not gift.get("telegram_gift_id"):
        return False, "Подарок не найден"
    try:
        await bot.send_gift(user_id=user_id, gift_id=gift["telegram_gift_id"], text=text or gift["title"])
        return True, "OK"
    except Exception as e:
        msg = str(e)
        if "DISALLOWED" in msg:
            return False, "🔒 Включи получение подарков"
        return False, msg[:80]


def roll_case(case_id: str, user_id: int = None) -> dict:
    case = CASES.get(case_id)
    if not case:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    if case.get("type") == "stars":
        winnable = [d for d in case["drops"] if d.get("can_win")]
        roll = random.random()
        cum = 0
        for d in winnable:
            cum += d["chance"]
            if roll < cum:
                return {"type": "stars", "stars_won": d["stars"], "all_drops": case["drops"]}
        return {"type": "stars", "stars_won": winnable[0]["stars"], "all_drops": case["drops"]}
    
    if case.get("pity_enabled") and user_id:
        new_spent = add_pity_spent(user_id, case["price"])
        if new_spent >= PITY_THRESHOLD:
            reset_pity(user_id)
            return {"type": "gift", "items": [PITY_REWARD_GIFT], "multiplier": 1, "pity_triggered": True, "pity_progress": 0}
        return {"type": "nothing", "items": [], "multiplier": 1, "pity_progress": new_spent}
    
    mult_info = case.get("multiplier")
    count = 1
    if mult_info and mult_info.get("enabled"):
        roll = random.random()
        cum = 0
        for opt in mult_info["chances"]:
            cum += opt["chance"]
            if roll < cum:
                count = opt["count"]
                break
    
    won = []
    for _ in range(count):
        roll = random.random()
        cum = 0
        result = "nothing"
        for d in case["drops"]:
            cum += d["chance"]
            if roll < cum:
                result = d["gift_id"]
                break
        if result != "nothing":
            won.append(result)
    
    if not won:
        return {"type": "nothing", "items": [], "multiplier": count}
    return {"type": "gift", "items": won, "multiplier": count}


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
        "👋 <b>Подарочница</b>\n\n🎁 Подарки • 🎰 Кейсы • ⭐ Stars\n\nНажми кнопку ниже!",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    payment = message.successful_payment
    payload = json.loads(payment.invoice_payload)
    buyer_id = message.from_user.id
    buyer_username = message.from_user.username
    total = payment.total_amount
    item_type = payload.get("type")
    payment_id = payload.get("payment_id")
    
    try:
        if item_type == "donate":
            save_donation(buyer_id, buyer_username, total)
            await message.answer(f"💝 Спасибо за {total}⭐!")
            return
        
        if item_type == "gift":
            gift = GIFTS[payload["id"]]
            text = format_gift_text(payload.get("sender"), buyer_username)
            success, error = await send_real_gift(buyer_id, payload["id"], text)
            save_purchase(buyer_id, {"type": "gift", "gift_id": payload["id"], "paid": total})
            await message.answer(f"🎉 {gift['title']}!" if success else f"⚠️ {error}")
            return
        
        if item_type == "case":
            case = CASES[payload["id"]]
            result = roll_case(payload["id"], buyer_id)
            if payment_id:
                save_pending_result(payment_id, result)
            
            if result["type"] == "stars":
                add_star_balance(buyer_id, result["stars_won"])
                await message.answer(f"⭐ +{result['stars_won']}⭐!", parse_mode=ParseMode.HTML)
            elif result["type"] == "nothing":
                pity = result.get("pity_progress", 0)
                text = f"😔 Ничего..." + (f"\n📊 До гарантии: {PITY_THRESHOLD - pity}⭐" if pity else "")
                await message.answer(text)
            else:
                for gid in result["items"]:
                    await send_real_gift(buyer_id, gid, f"Из {case['title']}")
                    await asyncio.sleep(0.2)
                await message.answer(f"🎉 {GIFTS[result['items'][0]]['title']}!")
            
            save_purchase(buyer_id, {"type": "case", "case_id": payload["id"], "paid": total})
    except Exception as e:
        print(f"❌ Payment: {e}")
        await message.answer(f"⚠️ Ошибка")


# ===== KEEP ALIVE =====
async def keep_alive():
    if not SELF_URL:
        return
    await asyncio.sleep(30)
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.get(f"{SELF_URL}/health", timeout=10)
            except:
                pass
            await asyncio.sleep(240)


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()
    await asyncio.sleep(1)
    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    print("✅ Готово!")
    yield
    print("👋 Стоп")


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# === MODELS ===
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


# === HELPERS ===
def raise_if_maintenance(uid: int):
    if is_maintenance_enabled() and not is_admin(uid):
        raise HTTPException(503, get_maintenance_text())


async def require_subscription(uid: int):
    sub = await check_subscription(uid)
    if not sub["subscribed"]:
        raise HTTPException(403, f"Подпишитесь на каналы")


# === ENDPOINTS ===

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
            chat_id = int(ch) if ch.lstrip('-').isdigit() else ch
            chat = await bot.get_chat(chat_id)
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


@app.get("/api/get-settings")
async def api_get_settings():
    return {"maintenance": is_maintenance_enabled(), "maintenance_text": get_maintenance_text()}


@app.get("/api/get-news")
async def api_get_news():
    return {"news": get_news()}


@app.get("/api/get-gifts")
async def api_get_gifts():
    return {
        "gifts": [{"id": gid, "title": g["title"], "price": g["price"], "gif_url": g["gif_url"]} for gid, g in GIFTS.items()],
        "sales": get_sales()
    }


@app.get("/api/get-cases")
async def api_get_cases():
    categories = {"cheap": {"title": "💰 Дешёвые", "cases": []}, "gifts": {"title": "🎁 Подарки", "cases": []}, "stars": {"title": "⭐ Stars", "cases": []}}
    for cid, c in CASES.items():
        cat = c.get("category", "gifts")
        data = {"id": cid, "title": c["title"], "price": c["price"], "category": cat, "type": c.get("type"), "pity_enabled": c.get("pity_enabled", False)}
        if cat in categories:
            categories[cat]["cases"].append(data)
    return {"categories": categories, "pityThreshold": PITY_THRESHOLD}


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    payment_id = str(uuid.uuid4())
    
    if req.giftId and req.giftId in GIFTS:
        gift = GIFTS[req.giftId]
        price = calc_gift_price(req.giftId, req.sender)["final_price"]
        link = await bot.create_invoice_link(
            title=gift["title"], description=gift["title"],
            payload=json.dumps({"type": "gift", "id": req.giftId, "sender": req.sender, "payment_id": payment_id}),
            currency="XTR", prices=[LabeledPrice(label=gift["title"], amount=price)]
        )
        return {"link": link, "paymentId": payment_id}
    
    if req.caseId and req.caseId in CASES:
        case = CASES[req.caseId]
        link = await bot.create_invoice_link(
            title=case["title"], description="🎰 Испытай удачу!",
            payload=json.dumps({"type": "case", "id": req.caseId, "payment_id": payment_id}),
            currency="XTR", prices=[LabeledPrice(label=case["title"], amount=case["price"])]
        )
        return {"link": link, "paymentId": payment_id}
    
    raise HTTPException(400, "Not found")


@app.post("/api/get-case-result")
async def api_get_case_result(req: GetResultReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    result = get_pending_result(req.paymentId)
    if not result:
        raise HTTPException(404, "Not found")
    
    uid = auth["user"]["id"]
    result["newBalance"] = get_star_balance(uid)
    result["pitySpent"] = get_pity_spent(uid)
    return result


@app.post("/api/buy-with-balance")
async def api_buy_with_balance(req: BuyWithBalanceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    username = auth["user"].get("username", "")
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    balance = get_star_balance(uid)
    
    if req.giftId and req.giftId in GIFTS:
        gift = GIFTS[req.giftId]
        price = calc_gift_price(req.giftId, req.sender)["final_price"]
        if balance < price:
            raise HTTPException(400, f"Нужно {price}⭐")
        
        use_star_balance(uid, price)
        success, error = await send_real_gift(uid, req.giftId, format_gift_text(req.sender, username))
        save_purchase(uid, {"type": "gift_balance", "gift_id": req.giftId, "paid": price})
        
        if not success:
            add_star_balance(uid, price)
            raise HTTPException(500, error)
        
        return {"success": True, "type": "gift", "reward": gift["title"], "newBalance": get_star_balance(uid)}
    
    if req.caseId and req.caseId in CASES:
        case = CASES[req.caseId]
        price = case["price"]
        if balance < price:
            raise HTTPException(400, f"Нужно {price}⭐")
        
        use_star_balance(uid, price)
        result = roll_case(req.caseId, uid)
        
        if result["type"] == "stars":
            add_star_balance(uid, result["stars_won"])
            return {"success": True, "type": "stars", "starsWon": result["stars_won"], "newBalance": get_star_balance(uid)}
        
        if result["type"] == "nothing":
            return {"success": True, "type": "nothing", "newBalance": get_star_balance(uid), "pitySpent": result.get("pity_progress", 0)}
        
        sent = []
        for gid in result["items"]:
            ok, _ = await send_real_gift(uid, gid, f"Из {case['title']}")
            if ok:
                sent.append({"id": gid, "title": GIFTS[gid]["title"]})
            await asyncio.sleep(0.2)
        
        return {"success": True, "type": "gifts", "won": sent, "multiplier": result["multiplier"], "newBalance": get_star_balance(uid)}
    
    raise HTTPException(400, "Не указан товар")


@app.post("/api/create-donate")
async def create_donate(req: DonateReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    if req.amount < 1 or req.amount > 10000:
        raise HTTPException(400, "1-10000")
    
    link = await bot.create_invoice_link(
        title="💝 Донат", description=f"{req.amount}⭐",
        payload=json.dumps({"type": "donate", "amount": req.amount}),
        currency="XTR", prices=[LabeledPrice(label="Донат", amount=req.amount)]
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
        raise HTTPException(404, "Не найден")
    
    promo = promos[code]
    if uid in promo.get("used_by", []):
        raise HTTPException(400, "Уже использован")
    if promo.get("uses", 0) >= promo.get("max_uses", 0):
        raise HTTPException(400, "Закончился")
    
    promo["uses"] = promo.get("uses", 0) + 1
    promo.setdefault("used_by", []).append(uid)
    save_promocode(code, promo)
    
    rt, ri = promo["reward_type"], promo["reward_id"]
    
    if rt == "stars":
        amount = int(ri)
        add_star_balance(uid, amount)
        return {"success": True, "reward": f"+{amount}⭐", "newBalance": get_star_balance(uid)}
    
    if rt == "gift":
        gift = GIFTS.get(ri)
        if gift:
            await send_real_gift(uid, ri, f"🎟 {code}")
            return {"success": True, "reward": gift["title"]}
    
    if rt == "case":
        result = roll_case(ri, uid)
        return {"success": True, "reward": f"Кейс", "caseResult": result}
    
    return {"success": True, "reward": "OK"}


@app.post("/api/admin/get-promocodes")
async def api_admin_get_promocodes(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    return {"promocodes": [{"code": c, **p} for c, p in get_promocodes().items()]}


@app.post("/api/admin/create-promocode")
async def api_admin_create_promocode(req: CreatePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    code = req.code.strip().upper()
    save_promocode(code, {
        "reward_type": req.rewardType, "reward_id": req.rewardId,
        "max_uses": req.maxUses, "uses": 0, "used_by": [],
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


@app.get("/api/clear-cache")
async def clear_cache():
    """Сбросить кэш"""
    CACHE["settings_time"] = 0
    CACHE["news_time"] = 0
    CACHE["sales_time"] = 0
    CACHE["promocodes_time"] = 0
    _load_all_cache()
    return {"status": "cache cleared"}


@app.get("/api/debug-settings")
async def debug_settings():
    """Отладка настроек"""
    return {
        "cache": CACHE.get("settings", {}),
        "cache_age": time.time() - CACHE.get("settings_time", 0),
        "maintenance": is_maintenance_enabled(),
        "maintenance_text": get_maintenance_text()
    }


@app.get("/api/test-channels")
async def test_channels():
    return {"channels": REQUIRED_CHANNELS, "count": len(REQUIRED_CHANNELS)}


@app.get("/")
async def root():
    return {"app": "Подарочница v7.2", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

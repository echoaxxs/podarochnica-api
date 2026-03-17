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
print("🚀 ПОДАРОЧНИЦА v8.0 — АВТОЗАГРУЗКА ПОДАРКОВ")
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

SENDERS = ["@echoaxxs", "@bogclm", "@bogclm и @echoaxxs"]
SIGNATURE_COSTS = {"@echoaxxs": 2, "@bogclm": 2, "@bogclm и @echoaxxs": 5}

PITY_THRESHOLD = 20
PITY_REWARD_GIFT = None  # Будет установлен после загрузки подарков

# ===== ПОДАРКИ (автозагрузка) =====
GIFTS = {}  # Заполняется автоматически из Telegram API

# Кастомные эмодзи по цене для названий
GIFT_EMOJIS = {
    15: ["❤️", "🧸", "🌸", "🍬", "🎀"],
    25: ["🌹", "🎁", "🍫", "🎈", "🌺"],
    50: ["💐", "🧁", "🍷", "🚀", "🎂"],
    100: ["💍", "🏆", "💎", "👑", "🌟"],
    250: ["🔥", "⚡", "🎭", "🦋", "🌈"],
    500: ["💫", "🎪", "🦄", "🌙", "✨"],
    1000: ["🏅", "🎖️", "💠", "🔮", "🌠"],
}

# ===== КЕЙСЫ =====
CASES = {
    "mini": {"title": "🎲 Мини", "price": 1, "category": "cheap", "pity_enabled": True},
    "basic-5": {"title": "🎯 Базовый 5", "price": 5, "category": "cheap", "pity_enabled": True},
    "basic-10": {"title": "🎯 Базовый 10", "price": 10, "category": "cheap", "pity_enabled": True},
    "basic-15": {"title": "🎯 Базовый 15", "price": 15, "category": "cheap", "pity_enabled": True},
    "premium": {"title": "💎 Премиум", "price": 50, "category": "gifts"},
    "rich": {"title": "💰 Богач", "price": 100, "category": "gifts"},
    "ultra": {"title": "🔥 Ультра", "price": 500, "category": "gifts", "multiplier": {"enabled": True, "chances": [{"count": 1, "chance": 0.50}, {"count": 2, "chance": 0.35}, {"count": 3, "chance": 0.15}]}},
    "star-100": {"title": "⭐ Star 100", "price": 100, "category": "stars", "type": "stars", "drops": [{"stars": 50, "chance": 0.70}, {"stars": 100, "chance": 0.25}, {"stars": 250, "chance": 0.05}]},
    "star-500": {"title": "⭐ Star 500", "price": 500, "category": "stars", "type": "stars", "drops": [{"stars": 250, "chance": 0.50}, {"stars": 500, "chance": 0.40}, {"stars": 1000, "chance": 0.10}]},
    "star-1000": {"title": "⭐ Star 1000", "price": 1000, "category": "stars", "type": "stars", "drops": [{"stars": 500, "chance": 0.40}, {"stars": 800, "chance": 0.35}, {"stars": 1000, "chance": 0.20}, {"stars": 1500, "chance": 0.05}]},
}

# ===== КЭШ =====
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
                    ws.append_row(["maintenance_text", "Идёт тех. перерыв, мы улучшаем подарочницу. Попробуй позже."])
        
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
        print(f"⚠️ Ошибка предзагрузки кэша: {e}")


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name) if spreadsheet else None
    except:
        return None


# ===== КЭШИРОВАННЫЕ ФУНКЦИИ =====

def _refresh_settings_cache():
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
    now = time.time()
    if now - CACHE.get("settings_time", 0) > CACHE_TTL:
        _refresh_settings_cache()
    return CACHE.get("settings", {}).get(key.lower(), default)


def is_maintenance_enabled() -> bool:
    value = get_setting("maintenance", "FALSE")
    return value.upper() in ("TRUE", "1", "YES", "ON", "ДА")


def get_maintenance_text() -> str:
    return get_setting("maintenance_text", "Идёт тех. перерыв, мы улучшаем подарочницу. Попробуй позже.")


def _refresh_news_cache():
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
    now = time.time()
    if now - CACHE.get("news_time", 0) > CACHE_TTL_LONG:
        _refresh_news_cache()
    return CACHE.get("news", [])


def _refresh_sales_cache():
    if not spreadsheet:
        return
    try:
        ws = get_sheet("sales")
        if not ws:
            return
        rows = ws.get_all_records()
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
    now = time.time()
    if now - CACHE.get("sales_time", 0) > CACHE_TTL_LONG:
        _refresh_sales_cache()
    return CACHE.get("sales", [])


def _refresh_balances_cache():
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


# ===== БАЛАНС =====

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


# ===== PITY =====

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


# ===== СОХРАНЕНИЕ =====

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
    """Автоматическая загрузка подарков из Telegram API"""
    global GIFTS, PITY_REWARD_GIFT
    
    try:
        available = await bot.get_available_gifts()
        if not available or not available.gifts:
            print("❌ Нет доступных подарков в Telegram")
            return False
        
        print(f"📦 Найдено {len(available.gifts)} подарков в Telegram API")
        
        # Группируем по цене для назначения названий
        by_price = {}
        for gift in available.gifts:
            by_price.setdefault(gift.star_count, []).append(gift)
        
        gifts_data = {}
        used_emojis = {price: 0 for price in GIFT_EMOJIS}
        cheapest_gift_id = None
        cheapest_price = float('inf')
        
        for gift in available.gifts:
            gift_id = gift.id
            price = gift.star_count
            sticker = gift.sticker
            
            # Определяем, лимитированный ли подарок
            is_limited = gift.total_count is not None and gift.total_count > 0
            remaining = gift.remaining_count if is_limited else None
            sold_out = is_limited and remaining == 0
            
            # Генерируем название
            if price in GIFT_EMOJIS:
                idx = used_emojis.get(price, 0)
                emojis = GIFT_EMOJIS[price]
                emoji = emojis[idx % len(emojis)]
                title = f"{emoji} Подарок {price}"
                used_emojis[price] = idx + 1
            else:
                title = f"🎁 Подарок {price}⭐"
            
            # Получаем URL стикера
            sticker_url = None
            thumbnail_url = None
            sticker_type = "static"
            
            try:
                if sticker.is_video:
                    sticker_type = "video"
                elif sticker.is_animated:
                    sticker_type = "animated"
                
                file = await bot.get_file(sticker.file_id)
                sticker_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
                
                if sticker.thumbnail:
                    thumb_file = await bot.get_file(sticker.thumbnail.file_id)
                    thumbnail_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{thumb_file.file_path}"
            except Exception as e:
                print(f"⚠️ Не удалось получить файл для {gift_id}: {e}")
            
            gifts_data[gift_id] = {
                "telegram_gift_id": gift_id,
                "title": title,
                "price": price,
                "star_cost": price,
                "sticker_url": sticker_url,
                "thumbnail_url": thumbnail_url,
                "sticker_type": sticker_type,
                "is_limited": is_limited,
                "total_count": gift.total_count,
                "remaining_count": remaining,
                "sold_out": sold_out,
            }
            
            # Ищем самый дешёвый для pity reward
            if price < cheapest_price and not sold_out:
                cheapest_price = price
                cheapest_gift_id = gift_id
        
        GIFTS = gifts_data
        PITY_REWARD_GIFT = cheapest_gift_id
        
        print(f"✅ Загружено {len(GIFTS)} подарков!")
        print(f"🎁 Pity reward: {PITY_REWARD_GIFT} ({cheapest_price}⭐)")
        
        # Статистика
        limited_count = sum(1 for g in GIFTS.values() if g["is_limited"])
        print(f"📊 Лимитированных: {limited_count}, Обычных: {len(GIFTS) - limited_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        import traceback
        traceback.print_exc()
        return False


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    gift = GIFTS.get(gift_id)
    if not gift:
        return False, "Подарок не найден"
    
    if gift.get("sold_out"):
        return False, "Подарок закончился"
    
    telegram_id = gift.get("telegram_gift_id", gift_id)
    
    try:
        await bot.send_gift(user_id=user_id, gift_id=telegram_id, text=text or gift["title"])
        return True, "OK"
    except Exception as e:
        msg = str(e)
        if "DISALLOWED" in msg.upper():
            return False, "🔒 Включи получение подарков"
        if "GIFT_SOLD_OUT" in msg.upper():
            # Обновляем статус
            if gift_id in GIFTS:
                GIFTS[gift_id]["sold_out"] = True
                GIFTS[gift_id]["remaining_count"] = 0
            return False, "😔 Подарок закончился"
        return False, msg[:80]


def get_available_gifts_for_case(category: str = "all") -> list:
    """Получить доступные подарки для кейса"""
    available = []
    for gid, g in GIFTS.items():
        if g.get("sold_out"):
            continue
        available.append({"id": gid, **g})
    
    # Сортируем по цене
    available.sort(key=lambda x: x["price"])
    return available


def roll_case(case_id: str, user_id: int = None) -> dict:
    case = CASES.get(case_id)
    if not case:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # Stars кейсы
    if case.get("type") == "stars":
        drops = case.get("drops", [])
        roll = random.random()
        cum = 0
        for d in drops:
            cum += d["chance"]
            if roll < cum:
                return {"type": "stars", "stars_won": d["stars"], "all_drops": drops}
        return {"type": "stars", "stars_won": drops[0]["stars"] if drops else 50, "all_drops": drops}
    
    # Pity кейсы (дешёвые)
    if case.get("pity_enabled") and user_id:
        new_spent = add_pity_spent(user_id, case["price"])
        if new_spent >= PITY_THRESHOLD and PITY_REWARD_GIFT:
            reset_pity(user_id)
            return {
                "type": "gift", 
                "items": [PITY_REWARD_GIFT], 
                "multiplier": 1, 
                "pity_triggered": True, 
                "pity_progress": 0
            }
        return {"type": "nothing", "items": [], "multiplier": 1, "pity_progress": new_spent}
    
    # Обычные кейсы с подарками
    available_gifts = get_available_gifts_for_case()
    if not available_gifts:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # Фильтруем по цене кейса
    case_price = case["price"]
    suitable_gifts = [g for g in available_gifts if g["price"] <= case_price]
    if not suitable_gifts:
        suitable_gifts = available_gifts[:5]  # Берём 5 самых дешёвых
    
    # Мультипликатор
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
    
    # Роллим подарки
    won = []
    for _ in range(count):
        # Шанс ничего не выиграть зависит от категории
        nothing_chance = 0.30 if case.get("category") == "gifts" else 0.05
        if random.random() < nothing_chance:
            continue
        
        # Выбираем случайный подарок
        gift = random.choice(suitable_gifts)
        won.append(gift["id"])
    
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
            gift = GIFTS.get(payload["id"])
            if not gift:
                await message.answer("⚠️ Подарок не найден")
                return
            text = format_gift_text(payload.get("sender"), buyer_username)
            success, error = await send_real_gift(buyer_id, payload["id"], text)
            save_purchase(buyer_id, {"type": "gift", "gift_id": payload["id"], "paid": total})
            await message.answer(f"🎉 {gift['title']}!" if success else f"⚠️ {error}")
            return
        
        if item_type == "case":
            case = CASES.get(payload["id"])
            if not case:
                await message.answer("⚠️ Кейс не найден")
                return
            result = roll_case(payload["id"], buyer_id)
            if payment_id:
                save_pending_result(payment_id, result)
            
            if result["type"] == "stars":
                add_star_balance(buyer_id, result["stars_won"])
                await message.answer(f"⭐ +{result['stars_won']}⭐!", parse_mode=ParseMode.HTML)
            elif result["type"] == "nothing":
                pity = result.get("pity_progress", 0)
                text = f"😔 Ничего..." + (f"\n📊 Продолжая крутить вам выпадет гарантированный подарок!" if pity else "")
                await message.answer(text)
            else:
                for gid in result["items"]:
                    gift = GIFTS.get(gid)
                    await send_real_gift(buyer_id, gid, f"Из {case['title']}")
                    await asyncio.sleep(0.2)
                first_gift = GIFTS.get(result["items"][0], {})
                await message.answer(f"🎉 {first_gift.get('title', 'Подарок')}!")
            
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


# ===== Периодическое обновление подарков =====
async def refresh_gifts_periodically():
    """Обновляем подарки каждые 10 минут"""
    await asyncio.sleep(60)  # Первый раз через минуту
    while True:
        try:
            await load_telegram_gifts()
        except Exception as e:
            print(f"⚠️ Ошибка обновления подарков: {e}")
        await asyncio.sleep(600)  # Каждые 10 минут


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()
    await asyncio.sleep(1)
    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    asyncio.create_task(refresh_gifts_periodically())
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
    """Возвращает все подарки с информацией о лимитах"""
    gifts_list = []
    for gid, g in GIFTS.items():
        gifts_list.append({
            "id": gid,
            "title": g["title"],
            "price": g["price"],
            "sticker_url": g.get("sticker_url"),
            "thumbnail_url": g.get("thumbnail_url"),
            "sticker_type": g.get("sticker_type", "static"),
            "is_limited": g.get("is_limited", False),
            "remaining": g.get("remaining_count"),
            "total": g.get("total_count"),
            "sold_out": g.get("sold_out", False),
        })
    
    # Сортируем: сначала обычные, потом лимитированные, по цене
    gifts_list.sort(key=lambda x: (x["sold_out"], x["is_limited"], x["price"]))
    
    return {
        "gifts": gifts_list,
        "sales": get_sales(),
        "total_count": len(gifts_list),
        "limited_count": sum(1 for g in gifts_list if g["is_limited"]),
    }


@app.get("/api/get-cases")
async def api_get_cases():
    categories = {
        "cheap": {"title": "💰 Дешёвые", "cases": []}, 
        "gifts": {"title": "🎁 Подарки", "cases": []}, 
        "stars": {"title": "⭐ Stars", "cases": []}
    }
    for cid, c in CASES.items():
        cat = c.get("category", "gifts")
        data = {
            "id": cid, 
            "title": c["title"], 
            "price": c["price"], 
            "category": cat, 
            "type": c.get("type"), 
            "pity_enabled": c.get("pity_enabled", False)
        }
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
        if gift.get("sold_out"):
            raise HTTPException(400, "Подарок закончился")
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
    
    # Добавляем информацию о выигранных подарках
    if result.get("items"):
        result["won"] = [
            {"id": gid, "title": GIFTS.get(gid, {}).get("title", "Подарок"), "price": GIFTS.get(gid, {}).get("price", 0)}
            for gid in result["items"]
        ]
    
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
        if gift.get("sold_out"):
            raise HTTPException(400, "Подарок закончился")
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
            gift = GIFTS.get(gid, {})
            ok, _ = await send_real_gift(uid, gid, f"Из {case['title']}")
            if ok:
                sent.append({"id": gid, "title": gift.get("title", "Подарок")})
            await asyncio.sleep(0.2)
        
        return {
            "success": True, 
            "type": "gifts", 
            "won": sent, 
            "multiplier": result["multiplier"], 
            "newBalance": get_star_balance(uid),
            "pityTriggered": result.get("pity_triggered", False)
        }
    
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
            success, error = await send_real_gift(uid, ri, f"🎟 {code}")
            if success:
                return {"success": True, "reward": gift["title"]}
            else:
                return {"success": False, "error": error}
    
    if rt == "case":
        result = roll_case(ri, uid)
        return {"success": True, "reward": f"Кейс", "caseResult": result}
    
    return {"success": True, "reward": "OK"}


@app.post("/api/admin/get-promocodes")
async def api_admin_get_promocodes(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    
    promos = []
    for code, p in get_promocodes().items():
        promos.append({
            "code": code,
            "rewardType": p.get("reward_type", ""),
            "rewardId": p.get("reward_id", ""),
            "maxUses": p.get("max_uses", 0),
            "uses": p.get("uses", 0),
            "created": p.get("created", ""),
        })
    
    return {"promocodes": promos}


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
    """Список подарков для админки"""
    return {
        "gifts": [
            {"id": gid, "title": g["title"], "price": g["price"], "is_limited": g.get("is_limited", False)}
            for gid, g in sorted(GIFTS.items(), key=lambda x: x[1]["price"])
        ],
        "cases": [
            {"id": cid, "title": c["title"], "price": c["price"]}
            for cid, c in CASES.items()
        ]
    }


@app.get("/api/clear-cache")
async def clear_cache():
    CACHE["settings_time"] = 0
    CACHE["news_time"] = 0
    CACHE["sales_time"] = 0
    CACHE["promocodes_time"] = 0
    _load_all_cache()
    return {"status": "cache cleared"}


@app.get("/api/refresh-gifts")
async def refresh_gifts():
    """Принудительное обновление подарков"""
    success = await load_telegram_gifts()
    return {
        "success": success,
        "gifts_count": len(GIFTS),
        "limited_count": sum(1 for g in GIFTS.values() if g.get("is_limited"))
    }


@app.get("/")
async def root():
    return {
        "app": "Подарочница v8.0",
        "status": "running",
        "gifts_loaded": len(GIFTS),
        "pity_reward": PITY_REWARD_GIFT
    }


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat(), "gifts": len(GIFTS)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

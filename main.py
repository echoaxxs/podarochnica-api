import os
import json
import hashlib
import hmac
import asyncio
import random
import uuid
import httpx
import gspread
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

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
print("🚀 ПОДАРОЧНИЦА v7.0 — ПОЛНАЯ ПЕРЕРАБОТКА")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

# Админы (могут создавать промокоды)
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# Обязательные каналы для подписки
REQUIRED_CHANNELS = [x.strip() for x in os.getenv("REQUIRED_CHANNELS", "").split(",") if x.strip()]
# Пример: REQUIRED_CHANNELS=@channel1,@channel2 или -1001234567890,-1009876543210

SENDERS = ["@echoaxxs", "@bogclm", "@bogclm и @echoaxxs"]
SIGNATURE_COSTS = {"@echoaxxs": 2, "@bogclm": 2, "@bogclm и @echoaxxs": 5}

# ===== PITY СИСТЕМА =====
PITY_THRESHOLD = 20  # После 20 звёзд на дешёвые кейсы - гарантированный подарок
PITY_REWARD_GIFT = "heart"  # Подарок за 15 звёзд
CHEAP_CASE_IDS = ["mini", "basic-5", "basic-10", "basic-15"]  # Дешёвые кейсы

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
    # === ДЕШЁВЫЕ (pity система) ===
    "mini": {
        "title": "🎲 Мини",
        "price": 1,
        "category": "cheap",
        "drops": [
            {"gift_id": "heart", "chance": 0.0},  # Шанс 0 - работает pity
            {"gift_id": "bear", "chance": 0.0},
        ],
        "pity_enabled": True
    },
    "basic-5": {
        "title": "🎯 Базовый 5",
        "price": 5,
        "category": "cheap",
        "drops": [
            {"gift_id": "heart", "chance": 0.0},
            {"gift_id": "bear", "chance": 0.0},
        ],
        "pity_enabled": True
    },
    "basic-10": {
        "title": "🎯 Базовый 10",
        "price": 10,
        "category": "cheap",
        "drops": [
            {"gift_id": "heart", "chance": 0.0},
            {"gift_id": "bear", "chance": 0.0},
        ],
        "pity_enabled": True
    },
    "basic-15": {
        "title": "🎯 Базовый 15",
        "price": 15,
        "category": "cheap",
        "drops": [
            {"gift_id": "heart", "chance": 0.0},
            {"gift_id": "bear", "chance": 0.0},
        ],
        "pity_enabled": True
    },
    
    # === ПОДАРОЧНЫЕ ===
    "premium": {
        "title": "💎 Премиум",
        "price": 50,
        "category": "gifts",
        "drops": [
            {"gift_id": "rose", "chance": 0.35},
            {"gift_id": "box", "chance": 0.35},
            {"gift_id": "nothing", "chance": 0.30},
        ]
    },
    "rich": {
        "title": "💰 Богач",
        "price": 100,
        "category": "gifts",
        "drops": [
            {"gift_id": "rose", "chance": 0.30},
            {"gift_id": "box", "chance": 0.30},
            {"gift_id": "brilliant_ring", "chance": 0.10},
            {"gift_id": "rocket", "chance": 0.15},
            {"gift_id": "nothing", "chance": 0.15},
        ]
    },
    "ultra": {
        "title": "🔥 Ультра",
        "price": 500,
        "category": "gifts",
        "drops": [
            {"gift_id": "brilliant_ring", "chance": 0.35},
            {"gift_id": "diamond", "chance": 0.30},
            {"gift_id": "heroic_cup", "chance": 0.30},
            {"gift_id": "nothing", "chance": 0.05},
        ],
        "multiplier": {
            "enabled": True,
            "chances": [
                {"count": 1, "chance": 0.50},
                {"count": 2, "chance": 0.35},
                {"count": 3, "chance": 0.15},
            ]
        }
    },
    
    # === STAR КЕЙСЫ ===
    "star-100": {
        "title": "⭐ Star 100",
        "price": 100,
        "category": "stars",
        "type": "stars",
        "drops": [
            {"stars": 50, "chance": 0.70, "can_win": True},
            {"stars": 100, "chance": 0.30, "can_win": True},
            {"stars": 250, "chance": 0.50, "can_win": False},
            {"stars": 500, "chance": 0.40, "can_win": False},
            {"stars": 1000, "chance": 0.02, "can_win": False},
            {"stars": 5000, "chance": 0.003, "can_win": False},
            {"stars": 10000, "chance": 0.001, "can_win": False},
        ]
    },
    "star-500": {
        "title": "⭐ Star 500",
        "price": 500,
        "category": "stars",
        "type": "stars",
        "drops": [
            {"stars": 250, "chance": 0.50, "can_win": True},
            {"stars": 500, "chance": 0.40, "can_win": True},
            {"stars": 1000, "chance": 0.08, "can_win": True},
            {"stars": 1500, "chance": 0.015, "can_win": False},
            {"stars": 5000, "chance": 0.004, "can_win": False},
            {"stars": 10000, "chance": 0.001, "can_win": False},
        ]
    },
    "star-1000": {
        "title": "⭐ Star 1000",
        "price": 1000,
        "category": "stars",
        "type": "stars",
        "drops": [
            {"stars": 500, "chance": 0.40, "can_win": True},
            {"stars": 800, "chance": 0.30, "can_win": True},
            {"stars": 1000, "chance": 0.20, "can_win": True},
            {"stars": 1500, "chance": 0.07, "can_win": True},
            {"stars": 3000, "chance": 0.02, "can_win": False},
            {"stars": 5000, "chance": 0.007, "can_win": False},
            {"stars": 10000, "chance": 0.003, "can_win": False},
        ]
    },
}

# ===== НОВОСТИ (для главной страницы) =====
NEWS = [
    {
        "id": "1",
        "title": "🎉 Запуск Подарочницы v7!",
        "text": "Новый дизайн, Star кейсы и система гарантированных подарков!",
        "image": "https://podarochnica.pages.dev/news/launch.png",
        "date": "2025-01-15"
    },
    {
        "id": "2", 
        "title": "⭐ Star кейсы",
        "text": "Открывай кейсы и выигрывай звёзды на баланс!",
        "image": "https://podarochnica.pages.dev/news/stars.png",
        "date": "2025-01-14"
    },
]


def format_gift_text(sender_key: str, recipient_username: str = None) -> str:
    if not sender_key or sender_key not in SENDERS:
        return None
    recipient = recipient_username.lstrip("@") if recipient_username else None
    return f"От {sender_key} для @{recipient}" if recipient else f"От {sender_key}"


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
        sheets_to_create = {
            "promocodes": ["code", "reward_type", "reward_id", "max_uses", "uses", "used_by", "created", "paid"],
            "balances": ["user_id", "stars"],
            "pity": ["user_id", "spent"],  # Для pity системы
            "purchases": ["user_id", "type", "item_id", "paid", "sender", "timestamp"],
            "donations": ["user_id", "username", "amount", "timestamp"],
            "news": ["id", "title", "text", "image", "date", "active"]
        }
        for name, headers in sheets_to_create.items():
            if name not in existing:
                ws = spreadsheet.add_worksheet(name, rows=5000, cols=10)
                ws.append_row(headers)
        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name) if spreadsheet else None
    except:
        return None


def get_news_from_sheet() -> list:
    """Получает активные новости напрямую из Google Sheets"""
    if not spreadsheet:
        return []
    
    try:
        ws = get_sheet("news")
        if not ws:
            return []
        
        rows = ws.get_all_records()
        news_list = []
        
        for row in rows:
            # Читаем только те, у которых active = TRUE (или пусто)
            is_active = str(row.get("active", "TRUE")).strip().upper()
            if is_active == "FALSE":
                continue
            
            # Если нет заголовка - пропускаем
            if not str(row.get("title", "")).strip():
                continue
                
            news_list.append({
                "id": str(row.get("id", "")),
                "title": str(row.get("title", "")),
                "text": str(row.get("text", "")),
                "image": str(row.get("image", "")) or None,
                "date": str(row.get("date", ""))
            })
        
        # Сортируем по дате (самые новые сверху)
        news_list.sort(key=lambda x: x["date"], reverse=True)
        return news_list
        
    except Exception as e:
        print(f"❌ Ошибка чтения новостей: {e}")
        return []
# ===== ПАМЯТЬ =====
MEMORY = {
    "promocodes": {},
    "balances": {},
    "pity": {},  # user_id -> spent amount on cheap cases
    "pending_results": {},  # payment_id -> result (для синхронизации рулетки)
}


# === PROMOCODES ===
def get_promocodes() -> dict:
    if not spreadsheet:
        return MEMORY.get("promocodes", {})
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
                "used_by": used_by,
                "created": str(row.get("created", "")),
                "paid": bool(row.get("paid", False))
            }
        return result
    except Exception as e:
        print(f"❌ get_promocodes: {e}")
        return {}


def save_promocode(code: str, promo: dict):
    MEMORY.setdefault("promocodes", {})[code] = promo
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
            json.dumps(promo.get("used_by", [])), promo.get("created", ""),
            "TRUE" if promo.get("paid", False) else "FALSE"
        ]
        if found_row:
            ws.update(f"A{found_row}:H{found_row}", [row_data])
        else:
            ws.append_row(row_data)
    except Exception as e:
        print(f"❌ save_promocode: {e}")


def delete_promocode(code: str):
    MEMORY.get("promocodes", {}).pop(code, None)
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


# === STAR BALANCE ===
def get_star_balance(user_id: int) -> int:
    if not spreadsheet:
        return MEMORY.get("balances", {}).get(str(user_id), 0)
    try:
        ws = get_sheet("balances")
        if not ws:
            return 0
        rows = ws.get_all_records()
        uid_str = str(user_id)
        for row in rows:
            if str(row.get("user_id", "")) == uid_str:
                return int(row.get("stars", 0))
        return 0
    except:
        return 0


def set_star_balance(user_id: int, amount: int):
    MEMORY.setdefault("balances", {})[str(user_id)] = amount
    if not spreadsheet:
        return
    try:
        ws = get_sheet("balances")
        if not ws:
            return
        uid_str = str(user_id)
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == uid_str:
                found_row = i + 1
                break
        if found_row:
            ws.update(f"B{found_row}", [[amount]])
        else:
            ws.append_row([uid_str, amount])
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


# === PITY СИСТЕМА ===
def get_pity_spent(user_id: int) -> int:
    """Получить сколько юзер потратил на дешёвые кейсы"""
    if not spreadsheet:
        return MEMORY.get("pity", {}).get(str(user_id), 0)
    try:
        ws = get_sheet("pity")
        if not ws:
            return 0
        rows = ws.get_all_records()
        uid_str = str(user_id)
        for row in rows:
            if str(row.get("user_id", "")) == uid_str:
                return int(row.get("spent", 0))
        return 0
    except:
        return 0


def set_pity_spent(user_id: int, amount: int):
    """Установить сколько юзер потратил на дешёвые кейсы"""
    MEMORY.setdefault("pity", {})[str(user_id)] = amount
    if not spreadsheet:
        return
    try:
        ws = get_sheet("pity")
        if not ws:
            return
        uid_str = str(user_id)
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if row and row[0] == uid_str:
                found_row = i + 1
                break
        if found_row:
            ws.update(f"B{found_row}", [[amount]])
        else:
            ws.append_row([uid_str, amount])
    except Exception as e:
        print(f"❌ set_pity_spent: {e}")


def add_pity_spent(user_id: int, amount: int) -> int:
    """Добавить потраченное и вернуть новое значение"""
    current = get_pity_spent(user_id)
    new_total = current + amount
    set_pity_spent(user_id, new_total)
    return new_total


def reset_pity(user_id: int):
    """Сбросить pity после выигрыша"""
    set_pity_spent(user_id, 0)


# === PURCHASES & DONATIONS ===
def save_purchase(user_id: int, data: dict):
    if not spreadsheet:
        MEMORY.setdefault("purchases", {}).setdefault(str(user_id), []).append({
            **data, "timestamp": datetime.now().isoformat()
        })
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
        MEMORY.setdefault("donations", []).append({
            "user_id": user_id, "username": username, 
            "amount": amount, "timestamp": datetime.now().isoformat()
        })
        return
    try:
        ws = get_sheet("donations")
        if ws:
            ws.append_row([str(user_id), username or "", amount, datetime.now().isoformat()])
    except:
        pass


# === PENDING RESULTS (для синхронизации рулетки) ===
def save_pending_result(payment_id: str, result: dict):
    """Сохранить результат для последующего получения фронтом"""
    MEMORY.setdefault("pending_results", {})[payment_id] = {
        **result,
        "timestamp": datetime.now().isoformat()
    }


def get_pending_result(payment_id: str) -> Optional[dict]:
    """Получить и удалить результат"""
    return MEMORY.get("pending_results", {}).pop(payment_id, None)


# ===== KEEP ALIVE =====
async def keep_alive():
    if not SELF_URL:
        return
    ping_url = f"{SELF_URL}/health"
    await asyncio.sleep(30)
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.get(ping_url, timeout=10)
            except:
                pass
            await asyncio.sleep(4 * 60)


# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

available_telegram_gifts = {}
gifts_loaded = False


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
    """Проверить подписку на обязательные каналы"""
    if not REQUIRED_CHANNELS:
        return {"subscribed": True, "missing": []}
    
    missing = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status in ["left", "kicked"]:
                missing.append(channel)
        except Exception as e:
            print(f"⚠️ Ошибка проверки подписки {channel}: {e}")
            # Если не можем проверить - пропускаем
            continue
    
    return {
        "subscribed": len(missing) == 0,
        "missing": missing
    }


async def load_telegram_gifts():
    global available_telegram_gifts, gifts_loaded
    try:
        gifts = await bot.get_available_gifts()
        if not gifts or not gifts.gifts:
            return False
        
        gifts_by_price = {}
        for gift in gifts.gifts:
            price = gift.star_count
            if price not in gifts_by_price:
                gifts_by_price[price] = []
            gifts_by_price[price].append(gift)
        
        for price, gift_list in gifts_by_price.items():
            available_telegram_gifts[price] = gift_list
        
        used_index = {}
        for gid, gdata in GIFTS.items():
            our_cost = gdata["star_cost"]
            if our_cost in gifts_by_price:
                idx = used_index.get(our_cost, 0)
                gift_list = gifts_by_price[our_cost]
                tg_gift = gift_list[idx % len(gift_list)]
                GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                used_index[our_cost] = idx + 1
            elif gifts_by_price:
                closest = min(gifts_by_price.keys(), key=lambda x: abs(x - our_cost))
                idx = used_index.get(closest, 0)
                gift_list = gifts_by_price[closest]
                tg_gift = gift_list[idx % len(gift_list)]
                GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                GIFTS[gid]["star_cost"] = closest
                used_index[closest] = idx + 1
        
        gifts_loaded = True
        print("✅ Подарки загружены!")
        return True
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        return False


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    gift = GIFTS.get(gift_id)
    if not gift:
        return False, f"Подарок {gift_id} не найден"
    tg_id = gift.get("telegram_gift_id")
    if not tg_id:
        return False, f"telegram_gift_id не установлен"
    try:
        await bot.send_gift(user_id=user_id, gift_id=tg_id, text=text or gift["title"])
        return True, "OK"
    except Exception as e:
        error_msg = str(e)
        if "DISALLOWED" in error_msg:
            return False, "🔒 Включи получение подарков в настройках Telegram"
        return False, error_msg[:100]


def roll_case(case_id: str, user_id: int = None) -> dict:
    """
    Возвращает результат кейса.
    Для дешёвых кейсов применяется pity система.
    """
    case = CASES.get(case_id)
    if not case:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # === STAR КЕЙС ===
    if case.get("type") == "stars":
        winnable = [d for d in case["drops"] if d.get("can_win")]
        roll = random.random()
        cumulative = 0
        for drop in winnable:
            cumulative += drop["chance"]
            if roll < cumulative:
                return {"type": "stars", "stars_won": drop["stars"], "all_drops": case["drops"]}
        return {"type": "stars", "stars_won": winnable[0]["stars"], "all_drops": case["drops"]}
    
    # === PITY СИСТЕМА для дешёвых кейсов ===
    if case.get("pity_enabled") and user_id:
        current_spent = get_pity_spent(user_id)
        new_spent = add_pity_spent(user_id, case["price"])
        
        # Если набрали порог - гарантированный подарок
        if new_spent >= PITY_THRESHOLD:
            reset_pity(user_id)
            return {
                "type": "gift",
                "items": [PITY_REWARD_GIFT],
                "multiplier": 1,
                "pity_triggered": True,
                "pity_progress": 0
            }
        else:
            # Ещё не набрали - ничего
            return {
                "type": "nothing",
                "items": [],
                "multiplier": 1,
                "pity_progress": new_spent
            }
    
    # === ОБЫЧНЫЙ КЕЙС ===
    multiplier_info = case.get("multiplier")
    gift_count = 1
    if multiplier_info and multiplier_info.get("enabled"):
        roll_multi = random.random()
        cumulative = 0
        for opt in multiplier_info["chances"]:
            cumulative += opt["chance"]
            if roll_multi < cumulative:
                gift_count = opt["count"]
                break
    
    won_gifts = []
    for _ in range(gift_count):
        roll = random.random()
        cumulative = 0
        won = "nothing"
        for drop in case["drops"]:
            cumulative += drop["chance"]
            if roll < cumulative:
                won = drop["gift_id"]
                break
        
        # Если выпало ничего при множителе > 1 - перекрутить
        if won == "nothing" and gift_count > 1:
            non_nothing = [d for d in case["drops"] if d["gift_id"] != "nothing"]
            if non_nothing:
                total = sum(d["chance"] for d in non_nothing)
                roll2 = random.random() * total
                cum2 = 0
                for d in non_nothing:
                    cum2 += d["chance"]
                    if roll2 < cum2:
                        won = d["gift_id"]
                        break
        
        if won != "nothing":
            won_gifts.append(won)
    
    if not won_gifts:
        return {"type": "nothing", "items": [], "multiplier": gift_count}
    
    return {"type": "gift", "items": won_gifts, "multiplier": gift_count}


# ===== КОМАНДА /start =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer(
        "👋 <b>Подарочница</b>\n\n"
        "🎁 Подарки • 🎰 Кейсы • ⭐ Star кейс\n\n"
        "Нажми кнопку ниже чтобы открыть приложение!",
        reply_markup=kb, 
        parse_mode=ParseMode.HTML
    )


# ===== ОПЛАТА =====
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
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, f"💝 Донат {total}⭐ от @{buyer_username or buyer_id}")
                except:
                    pass
            return
        
        if item_type == "gift":
            item_id = payload.get("id")
            sender_key = payload.get("sender")
            gift = GIFTS[item_id]
            sender_text = format_gift_text(sender_key, buyer_username)
            success, error = await send_real_gift(buyer_id, item_id, sender_text)
            save_purchase(buyer_id, {
                "type": "gift", "gift_id": item_id, 
                "paid": total, "sender": sender_key or ""
            })
            
            if success:
                await message.answer(f"🎉 {gift['title']} отправлен!")
            else:
                await message.answer(f"⚠️ Ошибка: {error}")
            return
        
        if item_type == "case":
            item_id = payload.get("id")
            case = CASES[item_id]
            result = roll_case(item_id, buyer_id)
            
            # Сохраняем результат для фронта
            if payment_id:
                save_pending_result(payment_id, result)
            
            # === STAR КЕЙС ===
            if result["type"] == "stars":
                stars_won = result["stars_won"]
                add_star_balance(buyer_id, stars_won)
                balance = get_star_balance(buyer_id)
                save_purchase(buyer_id, {
                    "type": "star_win", "case_id": item_id, 
                    "paid": total, "stars_won": stars_won
                })
                await message.answer(
                    f"⭐ <b>{case['title']}</b>\n\n🎉 +{stars_won}⭐\n💰 Баланс: {balance}⭐",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # === НИЧЕГО ===
            if result["type"] == "nothing":
                pity_progress = result.get("pity_progress", 0)
                pity_text = ""
                if case.get("pity_enabled") and pity_progress > 0:
                    remaining = PITY_THRESHOLD - pity_progress
                    pity_text = f"\n\n📊 До гарантии: {remaining}⭐"
                
                save_purchase(buyer_id, {"type": "case_lose", "case_id": item_id, "paid": total})
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n😔 Ничего...{pity_text}", 
                    parse_mode=ParseMode.HTML
                )
                return
            
            # === ПОДАРКИ ===
            is_jackpot = result["multiplier"] > 1
            is_pity = result.get("pity_triggered", False)
            won_gifts = result["items"]
            success_count = 0
            
            for gift_id in won_gifts:
                wg = GIFTS[gift_id]
                if is_pity:
                    text = f"🎁 Гарантированный подарок!"
                elif is_jackpot:
                    text = f"🔥 ДЖЕКПОТ! Из {case['title']}"
                else:
                    text = f"Из {case['title']}"
                
                success, _ = await send_real_gift(buyer_id, gift_id, text)
                if success:
                    success_count += 1
                await asyncio.sleep(0.3)
            
            save_purchase(buyer_id, {
                "type": "case_win", "case_id": item_id, 
                "gift_ids": won_gifts, "paid": total
            })
            
            if is_pity:
                wg = GIFTS[won_gifts[0]]
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n🎁 <b>ГАРАНТИЯ!</b>\n\n{wg['title']}!",
                    parse_mode=ParseMode.HTML
                )
            elif is_jackpot:
                counts = {}
                for g in won_gifts:
                    counts[g] = counts.get(g, 0) + 1
                gifts_text = "\n".join(
                    f"• {GIFTS[g]['title']}" + (f" x{c}" if c > 1 else "") 
                    for g, c in counts.items()
                )
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n🔥 <b>ДЖЕКПОТ x{result['multiplier']}!</b>\n\n{gifts_text}",
                    parse_mode=ParseMode.HTML
                )
            else:
                wg = GIFTS[won_gifts[0]]
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n🎉 {wg['title']}!", 
                    parse_mode=ParseMode.HTML
                )
    
    except Exception as e:
        print(f"❌ Payment error: {e}")
        await message.answer(f"⚠️ Ошибка: {e}")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()
    await asyncio.sleep(2)
    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
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
    rewardType: str  # "gift", "case", "stars"
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
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# === ENDPOINTS ===

@app.post("/api/check-subscription")
async def api_check_subscription(req: InitDataReq):
    """Проверить подписку на обязательные каналы"""
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    result = await check_subscription(uid)
    
    # Добавляем информацию о каналах
    channels_info = []
    for channel in REQUIRED_CHANNELS:
        try:
            chat = await bot.get_chat(channel)
            channels_info.append({
                "id": channel,
                "title": chat.title,
                "username": chat.username,
                "missing": channel in result["missing"]
            })
        except:
            channels_info.append({
                "id": channel,
                "title": channel,
                "username": None,
                "missing": channel in result["missing"]
            })
    
    return {
        "subscribed": result["subscribed"],
        "channels": channels_info
    }


@app.post("/api/get-user-data")
async def api_get_user_data(req: InitDataReq):
    """Получить данные пользователя: баланс, pity прогресс, админ"""
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    balance = get_star_balance(uid)
    pity_spent = get_pity_spent(uid)
    admin = is_admin(uid)
    
    return {
        "starBalance": balance,
        "pitySpent": pity_spent,
        "pityThreshold": PITY_THRESHOLD,
        "isAdmin": admin
    }


@app.get("/api/get-news")
async def api_get_news():
    """Получить новости для главной страницы"""
    return {"news": get_news_from_sheet()}


@app.get("/api/get-gifts")
async def api_get_gifts():
    """Получить список подарков"""
    return {
        "gifts": [
            {
                "id": gid,
                "title": g["title"],
                "price": g["price"],
                "gif_url": g["gif_url"]
            }
            for gid, g in GIFTS.items()
        ]
    }


@app.get("/api/get-cases")
async def api_get_cases():
    """Получить список кейсов по категориям"""
    categories = {
        "cheap": {"title": "💰 Дешёвые", "cases": []},
        "gifts": {"title": "🎁 Подарки", "cases": []},
        "stars": {"title": "⭐ Stars", "cases": []},
    }
    
    for cid, c in CASES.items():
        cat = c.get("category", "gifts")
        case_data = {
            "id": cid,
            "title": c["title"],
            "price": c["price"],
            "category": cat,
            "type": c.get("type"),
            "pity_enabled": c.get("pity_enabled", False),
            "has_multiplier": c.get("multiplier", {}).get("enabled", False),
        }
        
        # Добавляем возможные дропы для отображения
        if c.get("type") == "stars":
            case_data["possible_drops"] = [
                {"stars": d["stars"], "can_win": d["can_win"]} 
                for d in c["drops"]
            ]
        else:
            case_data["possible_drops"] = [
                {"gift_id": d["gift_id"]} 
                for d in c["drops"] if d["gift_id"] != "nothing"
            ]
        
        if cat in categories:
            categories[cat]["cases"].append(case_data)
    
    return {"categories": categories, "pityThreshold": PITY_THRESHOLD}


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    buyer_username = auth["user"].get("username", "")
    
    # Проверка подписки
    sub_check = await check_subscription(uid)
    if not sub_check["subscribed"]:
        raise HTTPException(403, "Subscription required")
    
    payment_id = str(uuid.uuid4())
    
    try:
        if req.giftId and req.giftId in GIFTS:
            gift = GIFTS[req.giftId]
            sig_cost = SIGNATURE_COSTS.get(req.sender, 0) if req.sender in SENDERS else 0
            price = gift["price"] + sig_cost
            desc = format_gift_text(req.sender, buyer_username) if req.sender in SENDERS else gift["title"]
            
            link = await bot.create_invoice_link(
                title=gift["title"], 
                description=desc or gift["title"],
                payload=json.dumps({
                    "type": "gift", "id": req.giftId, 
                    "sender": req.sender, "payment_id": payment_id
                }),
                currency="XTR", 
                prices=[LabeledPrice(label=gift["title"], amount=price)]
            )
            return {"link": link, "paymentId": payment_id}
        
        if req.caseId and req.caseId in CASES:
            case = CASES[req.caseId]
            if case.get("type") == "stars":
                desc = "⭐ Выиграй звёзды!"
            else:
                desc = "🎰 Испытай удачу!"
            
            link = await bot.create_invoice_link(
                title=case["title"], 
                description=desc,
                payload=json.dumps({
                    "type": "case", "id": req.caseId, 
                    "payment_id": payment_id
                }),
                currency="XTR", 
                prices=[LabeledPrice(label=case["title"], amount=case["price"])]
            )
            return {"link": link, "paymentId": payment_id}
        
        raise HTTPException(400, "Not found")
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/get-case-result")
async def api_get_case_result(req: GetResultReq):
    """Получить результат кейса после оплаты Stars"""
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    result = get_pending_result(req.paymentId)
    if not result:
        raise HTTPException(404, "Result not found or already retrieved")
    
    uid = auth["user"]["id"]
    
    # Добавляем актуальные данные
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
    
    # Проверка подписки
    sub_check = await check_subscription(uid)
    if not sub_check["subscribed"]:
        raise HTTPException(403, "Subscription required")
    
    balance = get_star_balance(uid)
    
    # === ПОКУПКА ПОДАРКА ===
    if req.giftId and req.giftId in GIFTS:
        gift = GIFTS[req.giftId]
        sig_cost = SIGNATURE_COSTS.get(req.sender, 0) if req.sender in SENDERS else 0
        price = gift["price"] + sig_cost
        
        if balance < price:
            raise HTTPException(400, f"Недостаточно звёзд. Нужно {price}, есть {balance}")
        
        use_star_balance(uid, price)
        sender_text = format_gift_text(req.sender, username)
        success, error = await send_real_gift(uid, req.giftId, sender_text)
        
        save_purchase(uid, {
            "type": "gift_balance", "gift_id": req.giftId, 
            "paid": price, "sender": req.sender or ""
        })
        
        new_balance = get_star_balance(uid)
        
        if success:
            return {
                "success": True, "type": "gift", 
                "reward": gift["title"], "newBalance": new_balance
            }
        else:
            # Возврат звёзд при ошибке
            add_star_balance(uid, price)
            raise HTTPException(500, f"Не удалось отправить: {error}")
    
    # === ПОКУПКА КЕЙСА ===
    if req.caseId and req.caseId in CASES:
        case = CASES[req.caseId]
        price = case["price"]
        
        if price == 0:
            raise HTTPException(400, "Бесплатный кейс")
        
        if balance < price:
            raise HTTPException(400, f"Недостаточно звёзд. Нужно {price}, есть {balance}")
        
        use_star_balance(uid, price)
        result = roll_case(req.caseId, uid)
        
        # === STAR КЕЙС ===
        if result["type"] == "stars":
            stars_won = result["stars_won"]
            add_star_balance(uid, stars_won)
            new_balance = get_star_balance(uid)
            
            save_purchase(uid, {
                "type": "star_case_balance", "case_id": req.caseId, 
                "paid": price, "stars_won": stars_won
            })
            
            return {
                "success": True, "type": "stars",
                "starsWon": stars_won,
                "allDrops": result.get("all_drops", []),
                "newBalance": new_balance
            }
        
        # === НИЧЕГО ===
        if result["type"] == "nothing":
            save_purchase(uid, {
                "type": "case_lose_balance", "case_id": req.caseId, "paid": price
            })
            return {
                "success": True, "type": "nothing",
                "newBalance": get_star_balance(uid),
                "pitySpent": result.get("pity_progress", 0)
            }
        
        # === ПОДАРКИ ===
        won_gifts = result["items"]
        sent = []
        failed = []
        is_pity = result.get("pity_triggered", False)
        
        for gift_id in won_gifts:
            wg = GIFTS[gift_id]
            if is_pity:
                text = "🎁 Гарантированный подарок!"
            elif result["multiplier"] > 1:
                text = f"🔥 ДЖЕКПОТ! Из {case['title']}"
            else:
                text = f"Из {case['title']}"
            
            success, _ = await send_real_gift(uid, gift_id, text)
            if success:
                sent.append({"id": gift_id, "title": wg["title"]})
            else:
                failed.append(gift_id)
            await asyncio.sleep(0.3)
        
        save_purchase(uid, {
            "type": "case_win_balance", "case_id": req.caseId, 
            "gift_ids": won_gifts, "paid": price
        })
        
        return {
            "success": True, "type": "gifts",
            "won": sent,
            "failed": len(failed),
            "multiplier": result["multiplier"],
            "pityTriggered": is_pity,
            "newBalance": get_star_balance(uid),
            "pitySpent": get_pity_spent(uid)
        }
    
    raise HTTPException(400, "Не указан подарок или кейс")


@app.post("/api/create-donate")
async def create_donate(req: DonateReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    if req.amount < 1 or req.amount > 10000:
        raise HTTPException(400, "Invalid amount")
    
    try:
        link = await bot.create_invoice_link(
            title="💝 Донат", 
            description=f"Поддержка на {req.amount}⭐",
            payload=json.dumps({"type": "donate", "amount": req.amount}),
            currency="XTR", 
            prices=[LabeledPrice(label="Донат", amount=req.amount)]
        )
        return {"link": link}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/activate-promocode")
async def api_activate_promocode(req: PromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    code = req.code.strip().upper()
    promos = get_promocodes()
    
    if code not in promos:
        raise HTTPException(404, "Промокод не найден")
    
    promo = promos[code]
    
    if uid in promo.get("used_by", []):
        raise HTTPException(400, "Уже использован")
    
    if promo.get("uses", 0) >= promo.get("max_uses", 0):
        raise HTTPException(400, "Закончился")
    
    rt, ri = promo["reward_type"], promo["reward_id"]
    
    if rt == "stars":
        try:
            amount = int(ri)
            add_star_balance(uid, amount)
            promo["uses"] = promo.get("uses", 0) + 1
            promo.setdefault("used_by", []).append(uid)
            save_promocode(code, promo)
            return {
                "success": True, 
                "reward": f"+{amount}⭐", 
                "newBalance": get_star_balance(uid)
            }
        except:
            raise HTTPException(500, "Invalid stars amount")
    
    if rt == "gift":
        gift = GIFTS.get(ri)
        if gift:
            success, error = await send_real_gift(uid, ri, f"🎟 Промокод {code}")
            promo["uses"] = promo.get("uses", 0) + 1
            promo.setdefault("used_by", []).append(uid)
            save_promocode(code, promo)
            if success:
                return {"success": True, "reward": gift["title"]}
            return {"success": True, "reward": gift["title"], "warning": error}
    
    if rt == "case":
        # Для кейсов просто крутим сразу
        result = roll_case(ri, uid)
        promo["uses"] = promo.get("uses", 0) + 1
        promo.setdefault("used_by", []).append(uid)
        save_promocode(code, promo)
        
        case = CASES.get(ri)
        title = case["title"] if case else ri
        
        return {
            "success": True, 
            "reward": f"Кейс: {title}",
            "caseResult": result
        }
    
    raise HTTPException(400, "Unknown reward type")


# === ADMIN ENDPOINTS ===

@app.post("/api/admin/get-promocodes")
async def api_admin_get_promocodes(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    if not is_admin(uid):
        raise HTTPException(403, "Admin only")
    
    promos = get_promocodes()
    return {
        "promocodes": [
            {
                "code": code,
                "rewardType": p["reward_type"],
                "rewardId": p["reward_id"],
                "maxUses": p["max_uses"],
                "uses": p["uses"],
                "created": p["created"]
            }
            for code, p in promos.items()
        ]
    }


@app.post("/api/admin/create-promocode")
async def api_admin_create_promocode(req: CreatePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    if not is_admin(uid):
        raise HTTPException(403, "Admin only")
    
    code = req.code.strip().upper()
    if not code or len(code) < 3:
        raise HTTPException(400, "Код должен быть минимум 3 символа")
    
    if req.rewardType not in ("case", "gift", "stars"):
        raise HTTPException(400, "Тип: case/gift/stars")
    
    if req.maxUses < 1:
        raise HTTPException(400, "Минимум 1 использование")
    
    promo = {
        "reward_type": req.rewardType,
        "reward_id": req.rewardId,
        "max_uses": req.maxUses,
        "uses": 0,
        "used_by": [],
        "created": datetime.now().isoformat(),
        "paid": True
    }
    save_promocode(code, promo)
    
    return {"success": True, "code": code}


@app.post("/api/admin/delete-promocode")
async def api_admin_delete_promocode(req: DeletePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    if not is_admin(uid):
        raise HTTPException(403, "Admin only")
    
    delete_promocode(req.code.upper())
    return {"success": True}


@app.get("/")
async def root():
    return {"app": "Подарочница v7.0", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

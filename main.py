import os
import json
import hashlib
import hmac
import asyncio
import random
import httpx
import gspread
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager
from typing import Optional, List, Tuple

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, LabeledPrice, PreCheckoutQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)
from aiogram.filters import Command, CommandObject
from aiogram.enums import ParseMode

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

print("=" * 50)
print("🚀 ПОДАРОЧНИЦА v6.0 — STAR КЕЙС + БАЛАНС")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

SENDERS = ["@echoaxxs", "@bogclm", "@bogclm и @echoaxxs"]
SIGNATURE_COSTS = {"@echoaxxs": 2, "@bogclm": 2, "@bogclm и @echoaxxs": 5}

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
    "mini": {
        "title": "🎲 Мини",
        "price": 1,
        "drops": [{"gift_id": "nothing", "chance": 1.00}]
    },
    "weekly": {
        "title": "📦 Еженедельный",
        "price": 0,
        "drops": [{"gift_id": "nothing", "chance": 1.00}]
    },
    "premium": {
        "title": "💎 Премиум",
        "price": 30,
        "drops": [
            {"gift_id": "rose", "chance": 0.35},
            {"gift_id": "box", "chance": 0.35},
            {"gift_id": "nothing", "chance": 0.30},
        ]
    },
    "rich": {
        "title": "💰 Богач",
        "price": 100,
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
    "star": {
        "title": "⭐ Star",
        "price": 1000,
        "type": "stars",
        "drops": [
            {"stars": 500, "chance": 0.40, "can_win": True},
            {"stars": 800, "chance": 0.30, "can_win": True},
            {"stars": 900, "chance": 0.15, "can_win": True},
            {"stars": 999, "chance": 0.05, "can_win": True},
            {"stars": 1000, "chance": 0.05, "can_win": True},
            {"stars": 1200, "chance": 0.02, "can_win": False},
            {"stars": 1500, "chance": 0.015, "can_win": False},
            {"stars": 3000, "chance": 0.01, "can_win": False},
            {"stars": 5000, "chance": 0.004, "can_win": False},
            {"stars": 10000, "chance": 0.001, "can_win": False},
        ]
    },
}

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
            "credits": ["user_id", "type", "item_id", "amount"],
            "balances": ["user_id", "stars"],
            "purchases": ["user_id", "type", "item_id", "paid", "sender", "timestamp"],
            "donations": ["user_id", "username", "amount", "timestamp"]
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

# ===== ПАМЯТЬ =====
MEMORY = {"promocodes": {}, "credits": {}, "balances": {}, "purchases": {}, "donations": [], "pending_promocodes": {}}

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
        row_data = [code, promo.get("reward_type", ""), promo.get("reward_id", ""),
                    promo.get("max_uses", 0), promo.get("uses", 0),
                    json.dumps(promo.get("used_by", [])), promo.get("created", ""),
                    "TRUE" if promo.get("paid", False) else "FALSE"]
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

# === CREDITS ===
def get_user_credits(user_id: int) -> dict:
    if not spreadsheet:
        return MEMORY.get("credits", {}).get(str(user_id), {"cases": {}, "gifts": {}})
    try:
        ws = get_sheet("credits")
        if not ws:
            return {"cases": {}, "gifts": {}}
        rows = ws.get_all_records()
        result = {"cases": {}, "gifts": {}}
        uid_str = str(user_id)
        for row in rows:
            if str(row.get("user_id", "")) == uid_str:
                item_type = str(row.get("type", ""))
                item_id = str(row.get("item_id", ""))
                amount = int(row.get("amount", 0))
                cat = "cases" if item_type == "case" else "gifts"
                result[cat][item_id] = amount
        return result
    except:
        return {"cases": {}, "gifts": {}}

def save_user_credit(user_id: int, item_type: str, item_id: str, amount: int):
    credits = MEMORY.setdefault("credits", {}).setdefault(str(user_id), {"cases": {}, "gifts": {}})
    cat = "cases" if item_type == "case" else "gifts"
    credits[cat][item_id] = amount
    if not spreadsheet:
        return
    try:
        ws = get_sheet("credits")
        if not ws:
            return
        uid_str = str(user_id)
        all_values = ws.get_all_values()
        found_row = None
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if len(row) >= 3 and row[0] == uid_str and row[1] == item_type and row[2] == item_id:
                found_row = i + 1
                break
        if found_row:
            if amount <= 0:
                ws.delete_rows(found_row)
            else:
                ws.update(f"D{found_row}", [[amount]])
        elif amount > 0:
            ws.append_row([uid_str, item_type, item_id, amount])
    except Exception as e:
        print(f"❌ save_credit: {e}")

def add_user_credit(user_id: int, item_type: str, item_id: str, amount: int = 1):
    credits = get_user_credits(user_id)
    cat = "cases" if item_type == "case" else "gifts"
    current = credits.get(cat, {}).get(item_id, 0)
    save_user_credit(user_id, item_type, item_id, current + amount)

def use_user_credit(user_id: int, item_type: str, item_id: str) -> bool:
    credits = get_user_credits(user_id)
    cat = "cases" if item_type == "case" else "gifts"
    current = credits.get(cat, {}).get(item_id, 0)
    if current <= 0:
        return False
    save_user_credit(user_id, item_type, item_id, current - 1)
    return True

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

# === PURCHASES & DONATIONS ===
def save_purchase(user_id: int, data: dict):
    if not spreadsheet:
        MEMORY.setdefault("purchases", {}).setdefault(str(user_id), []).append({**data, "timestamp": datetime.now().isoformat()})
        return
    try:
        ws = get_sheet("purchases")
        if ws:
            ws.append_row([str(user_id), data.get("type", ""), data.get("gift_id", data.get("case_id", "")),
                           data.get("paid", 0), data.get("sender", ""), datetime.now().isoformat()])
    except:
        pass

def save_donation(user_id: int, username: str, amount: int):
    if not spreadsheet:
        MEMORY.setdefault("donations", []).append({"user_id": user_id, "username": username, "amount": amount, "timestamp": datetime.now().isoformat()})
        return
    try:
        ws = get_sheet("donations")
        if ws:
            ws.append_row([str(user_id), username or "", amount, datetime.now().isoformat()])
    except:
        pass

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

def validate_init_data(init_data: str):
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

async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> Tuple[bool, str]:
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

def roll_case(case_id: str) -> dict:
    """Возвращает {"type": "gift"/"stars"/"nothing", "items": [...], "multiplier": N}"""
    case = CASES.get(case_id)
    if not case:
        return {"type": "nothing", "items": [], "multiplier": 1}
    
    # Star кейс
    if case.get("type") == "stars":
        winnable = [d for d in case["drops"] if d.get("can_win")]
        roll = random.random()
        cumulative = 0
        for drop in winnable:
            cumulative += drop["chance"]
            if roll < cumulative:
                return {"type": "stars", "items": [drop["stars"]], "multiplier": 1}
        return {"type": "stars", "items": [winnable[0]["stars"]], "multiplier": 1}
    
    # Обычный кейс
    multiplier = case.get("multiplier")
    gift_count = 1
    if multiplier and multiplier.get("enabled"):
        roll_multi = random.random()
        cumulative = 0
        for opt in multiplier["chances"]:
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

# ===== КОМАНДЫ =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    balance = get_star_balance(message.from_user.id)
    balance_text = f"\n⭐ Баланс: {balance}" if balance > 0 else ""
    await message.answer(
        f"👋 <b>Подарочница</b>{balance_text}\n\n"
        "🎁 Подарки • 🎰 Кейсы • ⭐ Star кейс",
        reply_markup=kb, parse_mode=ParseMode.HTML
    )

@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        return
    if not command.args:
        await message.answer(
            "📝 <b>Промокоды:</b>\n\n"
            "<code>/pr new КОД тип:id кол-во</code>\n"
            "Типы: case, gift, stars\n\n"
            "<code>/pr list</code>\n"
            "<code>/pr delete КОД</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    args = command.args.split()
    action = args[0].lower()
    
    if action == "new" and len(args) >= 4:
        code = args[1].upper()
        try:
            reward_type, reward_id = args[2].split(":", 1)
        except:
            await message.answer("❌ Формат: тип:id")
            return
        try:
            max_uses = int(args[3])
        except:
            await message.answer("❌ Количество = число")
            return
        
        if reward_type not in ("case", "gift", "stars"):
            await message.answer("❌ Тип: case/gift/stars")
            return
        
        promo = {"reward_type": reward_type, "reward_id": reward_id, "max_uses": max_uses,
                 "uses": 0, "used_by": [], "created": datetime.now().isoformat(), "paid": True}
        save_promocode(code, promo)
        await message.answer(f"✅ <code>{code}</code> создан", parse_mode=ParseMode.HTML)
    
    elif action == "list":
        promos = get_promocodes()
        if not promos:
            await message.answer("📭 Пусто")
            return
        text = "📋 <b>Промокоды:</b>\n\n"
        for c, p in promos.items():
            text += f"<code>{c}</code> — {p['reward_type']}:{p['reward_id']} ({p.get('uses', 0)}/{p.get('max_uses', 0)})\n"
        await message.answer(text, parse_mode=ParseMode.HTML)
    
    elif action == "delete" and len(args) >= 2:
        delete_promocode(args[1].upper())
        await message.answer("✅ Удалён")

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
            save_purchase(buyer_id, {"type": "gift", "gift_id": item_id, "paid": total, "sender": sender_key or ""})
            if success:
                await message.answer(f"🎉 {gift['title']} отправлен!")
            else:
                await message.answer(f"⚠️ Ошибка: {error}")
            return
        
        if item_type == "case":
            item_id = payload.get("id")
            case = CASES[item_id]
            result = roll_case(item_id)
            
            if result["type"] == "stars":
                stars_won = result["items"][0]
                add_star_balance(buyer_id, stars_won)
                balance = get_star_balance(buyer_id)
                save_purchase(buyer_id, {"type": "star_win", "case_id": item_id, "paid": total, "stars_won": stars_won})
                await message.answer(
                    f"⭐ <b>{case['title']}</b>\n\n🎉 +{stars_won}⭐\n💰 Баланс: {balance}⭐",
                    parse_mode=ParseMode.HTML
                )
                return
            
            if result["type"] == "nothing":
                save_purchase(buyer_id, {"type": "case_lose", "case_id": item_id, "paid": total})
                await message.answer(f"🎰 <b>{case['title']}</b>\n\n😔 Ничего...", parse_mode=ParseMode.HTML)
                return
            
            is_jackpot = result["multiplier"] > 1
            won_gifts = result["items"]
            success_count = 0
            
            for gift_id in won_gifts:
                wg = GIFTS[gift_id]
                text = f"{'🔥 ДЖЕКПОТ! ' if is_jackpot else ''}Из {case['title']}"
                success, _ = await send_real_gift(buyer_id, gift_id, text)
                if success:
                    success_count += 1
                await asyncio.sleep(0.3)
            
            save_purchase(buyer_id, {"type": "case_win", "case_id": item_id, "gift_ids": won_gifts, "paid": total})
            
            if is_jackpot:
                counts = {}
                for g in won_gifts:
                    counts[g] = counts.get(g, 0) + 1
                gifts_text = "\n".join(f"• {GIFTS[g]['title']}" + (f" x{c}" if c > 1 else "") for g, c in counts.items())
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n🔥 <b>ДЖЕКПОТ x{result['multiplier']}!</b>\n\n{gifts_text}",
                    parse_mode=ParseMode.HTML
                )
            else:
                wg = GIFTS[won_gifts[0]]
                await message.answer(f"🎰 <b>{case['title']}</b>\n\n🎉 {wg['title']}!", parse_mode=ParseMode.HTML)
    
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# === MODELS ===
class InvoiceReq(BaseModel):
    initData: str
    giftId: str | None = None
    caseId: str | None = None
    sender: str | None = None

class CreditsReq(BaseModel):
    initData: str

class UseCreditReq(BaseModel):
    initData: str
    itemType: str
    itemId: str

class PromocodeReq(BaseModel):
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

# === ENDPOINTS ===
@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    buyer_username = auth["user"].get("username", "")
    try:
        if req.giftId and req.giftId in GIFTS:
            gift = GIFTS[req.giftId]
            sig_cost = SIGNATURE_COSTS.get(req.sender, 0) if req.sender in SENDERS else 0
            price = gift["price"] + sig_cost
            desc = format_gift_text(req.sender, buyer_username) if req.sender in SENDERS else gift["title"]
            link = await bot.create_invoice_link(
                title=gift["title"], description=desc or gift["title"],
                payload=json.dumps({"type": "gift", "id": req.giftId, "sender": req.sender}),
                currency="XTR", prices=[LabeledPrice(label=gift["title"], amount=price)]
            )
            return {"link": link}
        
        if req.caseId and req.caseId in CASES:
            case = CASES[req.caseId]
            desc = "⭐ Выиграй звёзды!" if case.get("type") == "stars" else "🎰 Испытай удачу!"
            link = await bot.create_invoice_link(
                title=case["title"], description=desc,
                payload=json.dumps({"type": "case", "id": req.caseId}),
                currency="XTR", prices=[LabeledPrice(label=case["title"], amount=case["price"])]
            )
            return {"link": link}
        
        raise HTTPException(400, "Not found")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/create-donate")
async def create_donate(req: DonateReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    if req.amount < 1 or req.amount > 10000:
        raise HTTPException(400, "Invalid amount")
    try:
        link = await bot.create_invoice_link(
            title="💝 Донат", description=f"Поддержка на {req.amount}⭐",
            payload=json.dumps({"type": "donate", "amount": req.amount}),
            currency="XTR", prices=[LabeledPrice(label="Донат", amount=req.amount)]
        )
        return {"link": link}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/get-credits")
async def api_get_credits(req: CreditsReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    credits = get_user_credits(uid)
    balance = get_star_balance(uid)
    return {"credits": credits, "starBalance": balance}

@app.post("/api/use-credit")
async def api_use_credit(req: UseCreditReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    if use_user_credit(auth["user"]["id"], req.itemType, req.itemId):
        return {"success": True}
    raise HTTPException(400, "No credits")

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
            return {"success": True, "reward": f"+{amount}⭐", "newBalance": get_star_balance(uid)}
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
        add_user_credit(uid, "case", ri)
        promo["uses"] = promo.get("uses", 0) + 1
        promo.setdefault("used_by", []).append(uid)
        save_promocode(code, promo)
        title = CASES.get(ri, {}).get("title", ri)
        return {"success": True, "reward": title}
    
    raise HTTPException(400, "Unknown reward type")

@app.post("/api/buy-with-balance")
async def api_buy_with_balance(req: BuyWithBalanceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    
    uid = auth["user"]["id"]
    username = auth["user"].get("username", "")
    balance = get_star_balance(uid)
    
    # Покупка подарка
    if req.giftId and req.giftId in GIFTS:
        gift = GIFTS[req.giftId]
        sig_cost = SIGNATURE_COSTS.get(req.sender, 0) if req.sender in SENDERS else 0
        price = gift["price"] + sig_cost
        
        if balance < price:
            raise HTTPException(400, f"Недостаточно звёзд. Нужно {price}, есть {balance}")
        
        use_star_balance(uid, price)
        sender_text = format_gift_text(req.sender, username)
        success, error = await send_real_gift(uid, req.giftId, sender_text)
        
        save_purchase(uid, {"type": "gift_balance", "gift_id": req.giftId, "paid": price, "sender": req.sender or ""})
        
        new_balance = get_star_balance(uid)
        
        if success:
            return {"success": True, "type": "gift", "reward": gift["title"], "newBalance": new_balance}
        else:
            add_star_balance(uid, price)
            raise HTTPException(500, f"Не удалось отправить: {error}")
    
    # Покупка кейса
    if req.caseId and req.caseId in CASES:
        case = CASES[req.caseId]
        price = case["price"]
        
        if price == 0:
            raise HTTPException(400, "Бесплатный кейс")
        
        if balance < price:
            raise HTTPException(400, f"Недостаточно звёзд. Нужно {price}, есть {balance}")
        
        use_star_balance(uid, price)
        result = roll_case(req.caseId)
        
        # Star кейс
        if result["type"] == "stars":
            stars_won = result["items"][0]
            add_star_balance(uid, stars_won)
            new_balance = get_star_balance(uid)
            save_purchase(uid, {"type": "star_case_balance", "case_id": req.caseId, "paid": price, "stars_won": stars_won})
            return {"success": True, "type": "stars", "won": stars_won, "newBalance": new_balance}
        
        # Ничего
        if result["type"] == "nothing":
            save_purchase(uid, {"type": "case_lose_balance", "case_id": req.caseId, "paid": price})
            return {"success": True, "type": "nothing", "newBalance": get_star_balance(uid)}
        
        # Подарки
        won_gifts = result["items"]
        sent = []
        failed = []
        
        for gift_id in won_gifts:
            wg = GIFTS[gift_id]
            text = f"{'🔥 ДЖЕКПОТ! ' if result['multiplier'] > 1 else ''}Из {case['title']}"
            success, _ = await send_real_gift(uid, gift_id, text)
            if success:
                sent.append(wg["title"])
            else:
                failed.append(gift_id)
            await asyncio.sleep(0.3)
        
        save_purchase(uid, {"type": "case_win_balance", "case_id": req.caseId, "gift_ids": won_gifts, "paid": price})
        
        return {
            "success": True,
            "type": "gifts",
            "won": sent,
            "failed": len(failed),
            "multiplier": result["multiplier"],
            "newBalance": get_star_balance(uid)
        }
    
    raise HTTPException(400, "Не указан подарок или кейс")

@app.get("/")
async def root():
    return {"app": "Подарочница v6.0", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

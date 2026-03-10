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
from typing import Optional

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
print("🚀 ВЕРСИЯ 3.0 — ИСПРАВЛЕН MARKDOWN + КОНФЛИКТЫ")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

SENDERS = ["@echoaxxs", "@bogclm"]
SIGNATURE_COST = 2

GIFTS = {
    "rocket": {"title": "🚀 Ракета", "price": 50, "star_cost": 50, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/rocket.gif"},
    "rose": {"title": "🌹 Роза", "price": 25, "star_cost": 25, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/rose.gif"},
    "box": {"title": "🎁 Подарок", "price": 25, "star_cost": 25, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/gift.gif"},
    "heart": {"title": "❤️ Сердце", "price": 15, "star_cost": 15, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/heart.gif"},
    "bear": {"title": "🧸 Мишка", "price": 15, "star_cost": 15, "telegram_gift_id": None, "gif_url": "https://podarochnica.pages.dev/bear.gif"},
}

CASES = {
    "premium": {
        "title": "💎 Премиум кейс", "price": 30,
        "drops": [
            {"gift_id": "rose", "chance": 0.35},
            {"gift_id": "box", "chance": 0.35},
            {"gift_id": "rocket", "chance": 0.00},
            {"gift_id": "nothing", "chance": 0.40},
        ]
    },
    "rich": {
        "title": "💰 Кейс Богач", "price": 100,
        "drops": [
            {"gift_id": "rocket", "chance": 0.30},
            {"gift_id": "rose", "chance": 0.25},
            {"gift_id": "box", "chance": 0.25},
            {"gift_id": "heart", "chance": 0.10},
            {"gift_id": "nothing", "chance": 0.10},
        ]
    },
    "ultra": {
        "title": "🔥 Ультра кейс", "price": 500,
        "drops": [
            {"gift_id": "rocket", "chance": 0.50},
            {"gift_id": "rose", "chance": 0.20},
            {"gift_id": "box", "chance": 0.20},
            {"gift_id": "heart", "chance": 0.05},
            {"gift_id": "nothing", "chance": 0.05},
        ]
    },
}


def format_gift_text(sender_key: str, recipient_username: str = None) -> str:
    if not sender_key or sender_key not in SENDERS:
        return None
    if recipient_username:
        recipient = recipient_username.lstrip("@")
        return f"Для @{recipient} от {sender_key}"
    else:
        return f"От {sender_key}"


# ===== GOOGLE SHEETS =====
gs_client = None
spreadsheet = None

def init_google_sheets():
    global gs_client, spreadsheet

    if not GOOGLE_CREDENTIALS or not GOOGLE_SHEET_ID:
        print("⚠️ Google Sheets не настроен — работаем в памяти")
        return False

    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS)
        gs_client = gspread.service_account_from_dict(creds_dict)
        spreadsheet = gs_client.open_by_key(GOOGLE_SHEET_ID)
        print("✅ Google Sheets подключён!")

        existing = [ws.title for ws in spreadsheet.worksheets()]

        if "promocodes" not in existing:
            ws = spreadsheet.add_worksheet("promocodes", rows=1000, cols=10)
            ws.append_row(["code", "reward_type", "reward_id", "max_uses", "uses", "used_by", "created", "paid"])

        if "credits" not in existing:
            ws = spreadsheet.add_worksheet("credits", rows=5000, cols=5)
            ws.append_row(["user_id", "type", "item_id", "amount"])

        if "purchases" not in existing:
            ws = spreadsheet.add_worksheet("purchases", rows=10000, cols=8)
            ws.append_row(["user_id", "type", "item_id", "paid", "sender", "timestamp"])
            
        if "donations" not in existing:
            ws = spreadsheet.add_worksheet("donations", rows=5000, cols=5)
            ws.append_row(["user_id", "username", "amount", "timestamp"])

        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name)
    except Exception as e:
        print(f"❌ Лист '{name}': {e}")
        return None


# ===== ПАМЯТЬ (фоллбэк) =====
MEMORY = {"promocodes": {}, "credits": {}, "purchases": {}, "donations": [], "pending_promocodes": {}}


def get_promocodes() -> dict:
    if not spreadsheet:
        return MEMORY.get("promocodes", {})

    try:
        ws = get_sheet("promocodes")
        if not ws:
            return MEMORY.get("promocodes", {})

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
        return MEMORY.get("promocodes", {})


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
            code,
            promo.get("reward_type", ""),
            promo.get("reward_id", ""),
            promo.get("max_uses", 0),
            promo.get("uses", 0),
            json.dumps(promo.get("used_by", [])),
            promo.get("created", ""),
            "TRUE" if promo.get("paid", False) else "FALSE"
        ]

        if found_row:
            ws.update(f"A{found_row}:H{found_row}", [row_data])
        else:
            ws.append_row(row_data)

    except Exception as e:
        print(f"   ❌ Sheets save_promocode: {e}")


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
        print(f"   ❌ Sheets delete: {e}")


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

                if item_type == "case":
                    result["cases"][item_id] = amount
                elif item_type == "gift":
                    result["gifts"][item_id] = amount

        return result
    except Exception as e:
        print(f"❌ get_credits: {e}")
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
        else:
            if amount > 0:
                ws.append_row([uid_str, item_type, item_id, amount])

    except Exception as e:
        print(f"   ❌ Sheets credit: {e}")


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


def save_purchase(user_id: int, data: dict):
    if not spreadsheet:
        MEMORY.setdefault("purchases", {}).setdefault(str(user_id), []).append(
            {**data, "timestamp": datetime.now().isoformat()}
        )
        return

    try:
        ws = get_sheet("purchases")
        if not ws:
            return
        ws.append_row([
            str(user_id),
            data.get("type", ""),
            data.get("gift_id", data.get("case_id", "")),
            data.get("paid", 0),
            data.get("sender", ""),
            datetime.now().isoformat()
        ])
    except Exception as e:
        print(f"   ❌ Sheets purchase: {e}")


def save_donation(user_id: int, username: str, amount: int):
    if not spreadsheet:
        MEMORY.setdefault("donations", []).append({
            "user_id": user_id,
            "username": username,
            "amount": amount,
            "timestamp": datetime.now().isoformat()
        })
        return

    try:
        ws = get_sheet("donations")
        if not ws:
            return
        ws.append_row([
            str(user_id),
            username or "",
            amount,
            datetime.now().isoformat()
        ])
    except Exception as e:
        print(f"   ❌ Sheets donation: {e}")


# ===== KEEP ALIVE =====
async def keep_alive():
    if not SELF_URL:
        print("⚠️ SELF_URL не задан — keep-alive выключен")
        return

    ping_url = f"{SELF_URL}/health"
    print(f"🏓 Keep-alive: {ping_url}")
    await asyncio.sleep(30)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.get(ping_url, timeout=10)
                print(f"🏓 Ping OK @ {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print(f"🏓 Ping fail: {e}")
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
    """Загружает доступные Telegram подарки и маппит их на наши"""
    global available_telegram_gifts, gifts_loaded
    
    print("\n" + "="*50)
    print("📦 ЗАГРУЗКА TELEGRAM ПОДАРКОВ")
    print("="*50)
    
    try:
        gifts = await bot.get_available_gifts()
        
        if not gifts or not gifts.gifts:
            print("❌ API вернул пустой список подарков!")
            return False
        
        print(f"✅ Получено {len(gifts.gifts)} подарков от Telegram:")
        
        # Группируем по цене
        gifts_by_price = {}
        for gift in gifts.gifts:
            price = gift.star_count
            if price not in gifts_by_price:
                gifts_by_price[price] = []
            gifts_by_price[price].append(gift)
            print(f"   • ID: {gift.id}, Цена: {price}⭐")
        
        # Сохраняем все подарки
        for price, gift_list in gifts_by_price.items():
            available_telegram_gifts[price] = gift_list
        
        print(f"\n🎯 Доступные цены: {sorted(gifts_by_price.keys())}")
        print(f"\n🔗 Маппинг наших подарков:")
        
        # Маппим наши подарки на Telegram подарки
        for gid, gdata in GIFTS.items():
            our_cost = gdata["star_cost"]
            
            if our_cost in gifts_by_price:
                tg_gift = gifts_by_price[our_cost][0]
                GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                print(f"   ✅ {gdata['title']} ({our_cost}⭐) → TG ID: {tg_gift.id}")
            else:
                if gifts_by_price:
                    closest = min(gifts_by_price.keys(), key=lambda x: abs(x - our_cost))
                    tg_gift = gifts_by_price[closest][0]
                    GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                    GIFTS[gid]["star_cost"] = closest
                    print(f"   ⚠️ {gdata['title']} ({our_cost}⭐ → {closest}⭐) → TG ID: {tg_gift.id}")
                else:
                    print(f"   ❌ {gdata['title']} — НЕ ЗАМАПЛЕН")
        
        gifts_loaded = True
        print("\n" + "="*50 + "\n")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        import traceback
        traceback.print_exc()
        return False


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    """Отправляет реальный Telegram подарок."""
    gift = GIFTS.get(gift_id)
    if not gift:
        return False, f"Подарок {gift_id} не найден"

    tg_id = gift.get("telegram_gift_id")
    
    print(f"\n🎁 Отправка подарка:")
    print(f"   Наш ID: {gift_id}")
    print(f"   Telegram ID: {tg_id}")
    print(f"   Получатель: {user_id}")
    
    if not tg_id:
        error = f"telegram_gift_id не установлен для {gift_id}"
        print(f"   ❌ {error}")
        
        try:
            await bot.send_animation(
                chat_id=user_id, 
                animation=gift["gif_url"], 
                caption=f"🎁 {gift['title']}\n\n{text or ''}\n\n⚠️ Подарок временно недоступен"
            )
        except:
            pass
        return False, error

    try:
        print(f"   📤 Отправляю...")
        await bot.send_gift(
            user_id=user_id, 
            gift_id=tg_id, 
            text=text or gift["title"]
        )
        print(f"   ✅ Успех!")
        return True, "OK"
        
    except Exception as e:
        error_msg = str(e)
        print(f"   ❌ Ошибка: {error_msg}")
        
        try:
            await bot.send_animation(
                chat_id=user_id, 
                animation=gift["gif_url"], 
                caption=f"🎁 {gift['title']}\n\n{text or ''}"
            )
        except:
            pass
            
        return False, error_msg


def roll_case(case_id: str) -> Optional[str]:
    case = CASES.get(case_id)
    if not case:
        return None
    roll = random.random()
    cumulative = 0
    for drop in case["drops"]:
        cumulative += drop["chance"]
        if roll < cumulative:
            return drop["gift_id"]
    return "nothing"


# ===== КОМАНДЫ =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    print(f"👤 /start от {message.from_user.id}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть подарочницу", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer(
        "👋 <b>Привет! Это Подарочница!</b>\n\n"
        "🎁 Подарки за ⭐ Stars\n"
        "🎰 Кейсы с призами\n"
        "🎟 Промокоды\n\n"
        "/promocode КОД — активировать\n"
        "/mycredits — кредиты\n"
        "/d сумма — поддержать бота\n"
        "/myid — узнать ID",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    is_admin = "✅ Админ" if message.from_user.id in ADMIN_IDS else "❌ Не админ"
    sheets = "✅" if spreadsheet else "❌"
    await message.answer(
        f"👤 ID: <code>{message.from_user.id}</code>\n🔐 {is_admin}\n📊 Sheets: {sheets}",
        parse_mode=ParseMode.HTML
    )


@router.message(Command("mycredits"))
async def cmd_mycredits(message: Message):
    credits = get_user_credits(message.from_user.id)
    text = "💳 <b>Кредиты:</b>\n\n"
    has = False
    for cid, amt in credits.get("cases", {}).items():
        if amt > 0:
            text += f"📦 {CASES.get(cid, {}).get('title', cid)}: {amt}\n"
            has = True
    for gid, amt in credits.get("gifts", {}).items():
        if amt > 0:
            text += f"🎁 {GIFTS.get(gid, {}).get('title', gid)}: {amt}\n"
            has = True
    if not has:
        text += "Пусто!"
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("d"))
async def cmd_donate(message: Message, command: CommandObject):
    if not command.args:
        await message.answer(
            "💝 <b>Поддержать бота:</b>\n\n"
            "<code>/d 10</code> — задонатить 10 ⭐\n"
            "<code>/d 50</code> — задонатить 50 ⭐\n"
            "<code>/d 100</code> — задонатить 100 ⭐\n\n"
            "Минимум: 1 ⭐",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        amount = int(command.args.strip())
        if amount < 1:
            await message.answer("❌ Минимум 1 ⭐")
            return
        if amount > 10000:
            await message.answer("❌ Максимум 10000 ⭐")
            return
    except ValueError:
        await message.answer("❌ Укажи число: <code>/d 10</code>", parse_mode=ParseMode.HTML)
        return
    
    try:
        link = await bot.create_invoice_link(
            title="💝 Донат",
            description=f"Поддержка бота на {amount} ⭐",
            payload=json.dumps({"type": "donate", "amount": amount}),
            currency="XTR",
            prices=[LabeledPrice(label="Донат", amount=amount)]
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💝 Задонатить {amount} ⭐", url=link)]
        ])
        
        await message.answer(
            f"💝 <b>Донат {amount} ⭐</b>\n\nСпасибо за поддержку!",
            reply_markup=kb,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        print(f"❌ Donate error: {e}")
        await message.answer("❌ Ошибка создания платежа")


# ===== ДИАГНОСТИКА =====
@router.message(Command("giftcheck"))
async def cmd_giftcheck(message: Message):
    """Полная диагностика системы подарков"""
    text = "🔍 <b>Диагностика подарков:</b>\n\n"
    
    text += f"📦 Подарки загружены: {'✅' if gifts_loaded else '❌'}\n"
    text += f"🔢 TG подарков: {sum(len(v) for v in available_telegram_gifts.values())}\n\n"
    
    text += "<b>Маппинг:</b>\n"
    for gid, gdata in GIFTS.items():
        tg_id = gdata.get("telegram_gift_id")
        status = "✅" if tg_id else "❌"
        tg_id_str = str(tg_id) if tg_id else "НЕТ"
        text += f"{status} {gdata['title']}: <code>{tg_id_str}</code>\n"
    
    text += f"\n<b>Цены TG:</b> {sorted(available_telegram_gifts.keys())}"
    
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("testgift"))
async def cmd_testgift(message: Message, command: CommandObject):
    """Тест отправки подарка себе"""
    uid = message.from_user.id
    
    gift_id = command.args.strip() if command.args else "bear"
    
    if gift_id not in GIFTS:
        await message.answer(
            f"❌ Подарок <code>{gift_id}</code> не найден\n\n"
            f"Доступные: {', '.join(GIFTS.keys())}",
            parse_mode=ParseMode.HTML
        )
        return
    
    gift = GIFTS[gift_id]
    tg_id = gift.get("telegram_gift_id")
    tg_id_str = str(tg_id) if tg_id else "НЕТ"
    
    await message.answer(
        f"🧪 <b>Тест отправки:</b>\n\n"
        f"Подарок: {gift['title']}\n"
        f"Наш ID: <code>{gift_id}</code>\n"
        f"TG ID: <code>{tg_id_str}</code>\n"
        f"Получатель: <code>{uid}</code>\n\n"
        f"Отправляю...",
        parse_mode=ParseMode.HTML
    )
    
    success, error = await send_real_gift(uid, gift_id, "Тестовый подарок")
    
    if success:
        await message.answer("✅ <b>Подарок успешно отправлен!</b>", parse_mode=ParseMode.HTML)
    else:
        await message.answer(f"❌ <b>Ошибка:</b>\n\n<code>{error}</code>", parse_mode=ParseMode.HTML)


@router.message(Command("reloadgifts"))
async def cmd_reloadgifts(message: Message):
    """Перезагрузить подарки"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer("🔄 Перезагружаю подарки...")
    
    success = await load_telegram_gifts()
    
    if success:
        text = "✅ <b>Подарки перезагружены!</b>\n\n"
        for gid, gdata in GIFTS.items():
            tg_id = gdata.get("telegram_gift_id")
            tg_id_str = str(tg_id) if tg_id else "НЕТ"
            text += f"{'✅' if tg_id else '❌'} {gdata['title']}: <code>{tg_id_str}</code>\n"
        await message.answer(text, parse_mode=ParseMode.HTML)
    else:
        await message.answer("❌ Ошибка загрузки. Смотри логи.")


@router.message(Command("tggifts"))
async def cmd_tggifts(message: Message):
    """Показать все Telegram подарки"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        gifts = await bot.get_available_gifts()
        
        text = f"🎁 <b>Telegram Gifts ({len(gifts.gifts)}):</b>\n\n"
        
        by_price = {}
        for g in gifts.gifts:
            if g.star_count not in by_price:
                by_price[g.star_count] = []
            by_price[g.star_count].append(g.id)
        
        for price in sorted(by_price.keys()):
            ids = by_price[price]
            text += f"<b>{price}⭐:</b> {len(ids)} шт.\n"
            for gid in ids[:3]:
                text += f"  <code>{gid}</code>\n"
            if len(ids) > 3:
                text += f"  <i>...и ещё {len(ids)-3}</i>\n"
        
        await message.answer(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# ===== ПРОМОКОД ДЛЯ ЮЗЕРОВ =====
@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    uid = message.from_user.id
    username = message.from_user.username
    print(f"🎟 /promocode от {uid}, args: '{command.args}'")

    if not command.args:
        await message.answer("❌ Напиши: <code>/promocode КОД</code>", parse_mode=ParseMode.HTML)
        return

    code = command.args.strip().upper()
    promocodes = get_promocodes()

    if code not in promocodes:
        await message.answer(f"❌ Промокод <code>{code}</code> не найден!", parse_mode=ParseMode.HTML)
        return

    promo = promocodes[code]

    if uid in promo.get("used_by", []):
        await message.answer("⚠️ Уже использован!")
        return

    if promo.get("uses", 0) >= promo.get("max_uses", 0):
        await message.answer("❌ Закончился!")
        return

    rt, ri = promo["reward_type"], promo["reward_id"]

    # Для подарков - отправляем реальный подарок
    if rt == "gift":
        if not promo.get("paid", False):
            await message.answer("❌ Этот промокод ещё не оплачен создателем!")
            return
        
        gift = GIFTS.get(ri)
        if gift:
            text = f"🎟 Подарок по промокоду {code}"
            
            await message.answer(f"🎁 Отправляю {gift['title']}...")
            
            success, error = await send_real_gift(uid, ri, text)
            
            promo["uses"] = promo.get("uses", 0) + 1
            promo.setdefault("used_by", []).append(uid)
            save_promocode(code, promo)
            
            if success:
                await message.answer(
                    f"✅ <b>Промокод активирован!</b>\n\n"
                    f"🎁 Тебе отправлен: {gift['title']}!",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer(
                    f"⚠️ <b>Промокод активирован, но подарок не доставлен</b>\n\n"
                    f"Ошибка: {error}",
                    parse_mode=ParseMode.HTML
                )
            return
    
    # Для кейсов — выдаём кредит
    add_user_credit(uid, rt, ri)

    promo["uses"] = promo.get("uses", 0) + 1
    promo.setdefault("used_by", []).append(uid)
    save_promocode(code, promo)

    title = CASES.get(ri, {}).get("title", ri) if rt == "case" else GIFTS.get(ri, {}).get("title", ri)
    await message.answer(f"✅ <b>Активировано!</b>\n\n🎁 Получено: {title}", parse_mode=ParseMode.HTML)


# ===== АДМИН КОМАНДЫ =====
@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer(f"⛔ Не админ!\nID: <code>{uid}</code>", parse_mode=ParseMode.HTML)
        return

    if not command.args:
        await message.answer(
            "📝 <b>Промокоды:</b>\n\n"
            "<b>Создать:</b>\n"
            "<code>/pr new КОД тип:id кол-во</code>\n\n"
            "<b>Примеры:</b>\n"
            "<code>/pr new FREEBEAR gift:bear 5</code>\n"
            "<code>/pr new FREECASE case:premium 10</code>\n\n"
            "<b>Типы:</b>\n"
            "🎁 gift: rocket/rose/box/heart/bear\n"
            "📦 case: premium/rich/ultra\n\n"
            "<code>/pr list</code> — список\n"
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
            await message.answer("❌ Формат: <code>тип:id</code>", parse_mode=ParseMode.HTML)
            return

        try:
            max_uses = int(args[3])
        except:
            await message.answer("❌ Количество = число!")
            return

        if reward_type not in ("case", "gift"):
            await message.answer("❌ Тип: case или gift")
            return

        if reward_type == "case" and reward_id not in CASES:
            await message.answer(f"❌ Нет кейса <code>{reward_id}</code>", parse_mode=ParseMode.HTML)
            return

        if reward_type == "gift" and reward_id not in GIFTS:
            await message.answer(f"❌ Нет подарка <code>{reward_id}</code>", parse_mode=ParseMode.HTML)
            return

        # Для подарков — нужна оплата
        if reward_type == "gift":
            gift = GIFTS[reward_id]
            total_cost = gift["star_cost"] * max_uses
            
            MEMORY.setdefault("pending_promocodes", {})[code] = {
                "reward_type": reward_type,
                "reward_id": reward_id,
                "max_uses": max_uses,
                "uses": 0,
                "used_by": [],
                "created": datetime.now().isoformat(),
                "paid": False,
                "creator_id": uid
            }
            
            try:
                link = await bot.create_invoice_link(
                    title=f"🎟 Промокод {code}",
                    description=f"{max_uses}× {gift['title']} для раздачи",
                    payload=json.dumps({
                        "type": "promocode_payment",
                        "code": code,
                        "gift_id": reward_id,
                        "count": max_uses
                    }),
                    currency="XTR",
                    prices=[LabeledPrice(label=f"{max_uses}× {gift['title']}", amount=total_cost)]
                )
                
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text=f"💳 Оплатить {total_cost} ⭐", url=link)]
                ])
                
                await message.answer(
                    f"🎟 <b>Промокод {code}</b>\n\n"
                    f"🎁 {max_uses}× {gift['title']}\n"
                    f"💰 Стоимость: {total_cost} ⭐\n\n"
                    f"Оплати чтобы активировать:",
                    reply_markup=kb,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                print(f"❌ Invoice error: {e}")
                await message.answer("❌ Ошибка создания платежа")
            return

        # Для кейсов — бесплатно
        promo = {
            "reward_type": reward_type,
            "reward_id": reward_id,
            "max_uses": max_uses,
            "uses": 0,
            "used_by": [],
            "created": datetime.now().isoformat(),
            "paid": True
        }

        save_promocode(code, promo)

        title = CASES.get(reward_id, {}).get("title", reward_id)
        await message.answer(
            f"✅ <b>Создан!</b>\n\n🎟 <code>{code}</code> → {title} (x{max_uses})\n\nЮзеры: <code>/promocode {code}</code>",
            parse_mode=ParseMode.HTML
        )

    elif action == "list":
        promocodes = get_promocodes()
        if not promocodes:
            await message.answer("📭 Пусто")
            return
        text = "📋 <b>Промокоды:</b>\n\n"
        for c, p in promocodes.items():
            paid_status = "✅" if p.get("paid", False) else "❌ не оплачен"
            text += f"<code>{c}</code> — {p.get('uses', 0)}/{p.get('max_uses', 0)} {paid_status}\n"
        await message.answer(text, parse_mode=ParseMode.HTML)

    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        delete_promocode(code)
        await message.answer(f"✅ <code>{code}</code> удалён", parse_mode=ParseMode.HTML)


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    sheets = "✅" if spreadsheet else "❌"
    promos = len(get_promocodes())
    gifts_status = "✅" if gifts_loaded else "❌"
    mapped = sum(1 for g in GIFTS.values() if g.get("telegram_gift_id"))
    
    await message.answer(
        f"🏓 Pong!\n"
        f"📊 Sheets: {sheets}\n"
        f"🎟 Промо: {promos}\n"
        f"🎁 Подарки: {gifts_status}\n"
        f"🔗 Замаплено: {mapped}/{len(GIFTS)}\n"
        f"👥 Админы: {len(ADMIN_IDS)}"
    )


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    text = f"🔧 <b>Debug:</b>\n\n"
    text += f"WEBAPP: <code>{WEBAPP_URL}</code>\n"
    text += f"ADMINS: <code>{ADMIN_IDS}</code>\n"
    text += f"Sheets: {'✅' if spreadsheet else '❌'}\n"
    text += f"Gifts loaded: {gifts_loaded}\n"
    text += f"Prices: {sorted(available_telegram_gifts.keys())}\n\n"
    text += "<b>Mapping:</b>\n"
    for gid, gdata in GIFTS.items():
        tg_id = gdata.get('telegram_gift_id')
        tg_str = str(tg_id) if tg_id else "НЕТ"
        text += f"{gdata['title']}: <code>{tg_str}</code>\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)


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
    
    print(f"\n💰 ПЛАТЁЖ от {buyer_id} (@{buyer_username}): {item_type}, {total}⭐")

    try:
        # ===== ДОНАТ =====
        if item_type == "donate":
            save_donation(buyer_id, buyer_username, total)
            await message.answer(
                f"💝 <b>Спасибо за донат {total} ⭐!</b>\n\nТы лучший! 🙏",
                parse_mode=ParseMode.HTML
            )
            
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"💝 Новый донат!\n👤 @{buyer_username or buyer_id}\n⭐ {total} Stars"
                    )
                except:
                    pass
            return
        
        # ===== ОПЛАТА ПРОМОКОДА =====
        if item_type == "promocode_payment":
            code = payload.get("code")
            pending = MEMORY.get("pending_promocodes", {}).get(code)
            
            if pending:
                pending["paid"] = True
                save_promocode(code, pending)
                MEMORY.get("pending_promocodes", {}).pop(code, None)
                
                gift = GIFTS.get(pending["reward_id"], {})
                await message.answer(
                    f"✅ <b>Промокод {code} оплачен!</b>\n\n"
                    f"🎁 {pending['max_uses']}× {gift.get('title', pending['reward_id'])}\n\n"
                    f"Раздавай: <code>/promocode {code}</code>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer("✅ Оплата прошла, но промокод не найден.")
            return

        # ===== ПОКУПКА ПОДАРКА =====
        if item_type == "gift":
            item_id = payload.get("id")
            sender_key = payload.get("sender")
            
            gift = GIFTS[item_id]
            sender_text = format_gift_text(sender_key, buyer_username)
            
            await message.answer(f"🎁 Отправляю {gift['title']}...")
            
            success, error = await send_real_gift(buyer_id, item_id, sender_text)
            
            save_purchase(buyer_id, {
                "type": "gift",
                "gift_id": item_id,
                "paid": total,
                "sender": sender_key or "",
                "success": success
            })
            
            if success:
                msg = f"🎉 {gift['title']} отправлен!"
                if sender_text:
                    msg += f"\n📝 {sender_text}"
                await message.answer(msg)
            else:
                await message.answer(
                    f"⚠️ Оплата прошла, но подарок не доставлен.\n\nОшибка: {error}"
                )
            return

        # ===== ПОКУПКА КЕЙСА =====
        if item_type == "case":
            item_id = payload.get("id")
            case = CASES[item_id]
            won = roll_case(item_id)

            if won and won != "nothing":
                wg = GIFTS[won]
                case_text = f"Из {case['title']}" + (f" для @{buyer_username}" if buyer_username else "")
                
                await message.answer(f"🎰 Выпало: {wg['title']}! Отправляю...")
                
                success, error = await send_real_gift(buyer_id, won, case_text)
                
                save_purchase(buyer_id, {
                    "type": "case_win",
                    "case_id": item_id,
                    "gift_id": won,
                    "paid": total,
                    "success": success
                })
                
                if success:
                    await message.answer(
                        f"🎰 <b>{case['title']}</b>\n\n🎉 {wg['title']} отправлен!",
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await message.answer(
                        f"🎰 <b>{case['title']}</b>\n\n"
                        f"🎉 Выпало: {wg['title']}!\n"
                        f"⚠️ Но не удалось отправить",
                        parse_mode=ParseMode.HTML
                    )
            else:
                save_purchase(buyer_id, {"type": "case_lose", "case_id": item_id, "paid": total})
                await message.answer(
                    f"🎰 <b>{case['title']}</b>\n\n😔 Ничего...",
                    parse_mode=ParseMode.HTML
                )
            return
            
    except Exception as e:
        print(f"❌ Payment error: {e}")
        import traceback
        traceback.print_exc()
        await message.answer(f"✅ Оплата прошла!\n\n⚠️ Ошибка: {e}")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("\n" + "="*60)
    print("🚀 ЗАПУСК ПОДАРОЧНИЦЫ v3.0")
    print("="*60)
    print(f"🔧 BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
    print(f"🔧 WEBAPP_URL: {WEBAPP_URL}")
    print(f"🔧 ADMIN_IDS: {ADMIN_IDS}")

    init_google_sheets()
    await load_telegram_gifts()

    print("⏳ Ждём 3 сек...")
    await asyncio.sleep(3)

    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    
    print("="*60)
    print("✅ БОТ РАБОТАЕТ!")
    print("="*60 + "\n")
    
    yield
    print("👋 Остановка")


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


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


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")

    buyer_username = auth["user"].get("username", "")

    try:
        if req.giftId and req.giftId in GIFTS:
            gift = GIFTS[req.giftId]
            price = gift["price"] + (SIGNATURE_COST if req.sender else 0)
            
            if req.sender and req.sender in SENDERS:
                desc = format_gift_text(req.sender, buyer_username) or gift["title"]
            else:
                desc = gift["title"]

            link = await bot.create_invoice_link(
                title=gift["title"],
                description=desc,
                payload=json.dumps({"type": "gift", "id": req.giftId, "sender": req.sender}),
                currency="XTR",
                prices=[LabeledPrice(label=gift["title"], amount=price)]
            )
            return {"link": link}

        elif req.caseId and req.caseId in CASES:
            case = CASES[req.caseId]
            link = await bot.create_invoice_link(
                title=case["title"],
                description="Открой и выиграй!",
                payload=json.dumps({"type": "case", "id": req.caseId}),
                currency="XTR",
                prices=[LabeledPrice(label=case["title"], amount=case["price"])]
            )
            return {"link": link}

        raise HTTPException(400, "Not found")
    except Exception as e:
        print(f"Invoice error: {e}")
        raise HTTPException(500, str(e))


@app.post("/api/get-credits")
async def api_get_credits(req: CreditsReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    return get_user_credits(auth["user"]["id"])


@app.post("/api/use-credit")
async def api_use_credit(req: UseCreditReq):
    auth = validate_init_data(req.initData)
    if not auth:
        raise HTTPException(401, "Invalid auth")
    if use_user_credit(auth["user"]["id"], req.itemType, req.itemId):
        return {"success": True}
    raise HTTPException(400, "No credits")


@app.head("/")
@app.get("/")
async def root():
    return {
        "app": "Подарочница v3.0", 
        "status": "running",
        "gifts_loaded": gifts_loaded
    }


@app.head("/health")
@app.get("/health")
async def health():
    return {
        "status": "ok", 
        "sheets": bool(spreadsheet), 
        "gifts_loaded": gifts_loaded,
        "time": datetime.now().isoformat()
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

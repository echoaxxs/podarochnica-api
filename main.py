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


# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

# Несколько админов через запятую: "123456,789012,345678"
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


# ===== ПОДПИСИ =====
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
            print("  ✓ Создан лист promocodes")

        if "credits" not in existing:
            ws = spreadsheet.add_worksheet("credits", rows=5000, cols=5)
            ws.append_row(["user_id", "type", "item_id", "amount"])
            print("  ✓ Создан лист credits")

        if "purchases" not in existing:
            ws = spreadsheet.add_worksheet("purchases", rows=10000, cols=8)
            ws.append_row(["user_id", "type", "item_id", "paid", "sender", "timestamp"])
            print("  ✓ Создан лист purchases")
            
        if "donations" not in existing:
            ws = spreadsheet.add_worksheet("donations", rows=5000, cols=5)
            ws.append_row(["user_id", "username", "amount", "timestamp"])
            print("  ✓ Создан лист donations")

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

@router.message(Command("testgift"))
async def cmd_testgift(message: Message):
    """Тест отправки подарка"""
    uid = message.from_user.id
    gift = GIFTS.get("bear")
    tg_id = gift.get("telegram_gift_id")
    
    await message.answer(f"🧪 gift_id: `{tg_id}`\nОтправляю...", parse_mode=ParseMode.MARKDOWN)
    
    try:
        await bot.send_gift(user_id=uid, gift_id=tg_id, text="Тест")
        await message.answer("✅ Успех!")
    except Exception as e:
        await message.answer(f"❌ Ошибка:\n`{e}`", parse_mode=ParseMode.MARKDOWN)


# ===== ПРОМОКОДЫ =====
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

        print(f"   ✅ Sheets: промокод {code} сохранён")
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
                print(f"   ✅ Sheets: {code} удалён")
                return
    except Exception as e:
        print(f"   ❌ Sheets delete: {e}")


# ===== КРЕДИТЫ =====
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


# ===== ПОКУПКИ =====
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


# ===== ДОНАТЫ =====
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
    global available_telegram_gifts
    try:
        gifts = await bot.get_available_gifts()
        print(f"📦 Загружено {len(gifts.gifts)} Telegram подарков:")
        
        # Сохраняем ВСЕ подарки по цене (список, а не один)
        gifts_by_price = {}
        for gift in gifts.gifts:
            price = gift.star_count
            if price not in gifts_by_price:
                gifts_by_price[price] = []
            gifts_by_price[price].append(gift)
            print(f"   • {gift.id}: {price}⭐")
        
        # Сохраняем для быстрого доступа (первый по каждой цене)
        for price, gift_list in gifts_by_price.items():
            available_telegram_gifts[price] = gift_list[0]
        
        print(f"\n🎯 Маппинг наших подарков:")
        
        # Маппим наши подарки
        for gid, gdata in GIFTS.items():
            our_cost = gdata["star_cost"]
            
            if our_cost in gifts_by_price:
                # Берём первый подарок с нужной ценой
                tg_gift = gifts_by_price[our_cost][0]
                GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                print(f"   ✅ {gdata['title']} ({our_cost}⭐) → {tg_gift.id}")
            else:
                # Ищем ближайший
                if gifts_by_price:
                    closest = min(gifts_by_price.keys(), key=lambda x: abs(x - our_cost))
                    tg_gift = gifts_by_price[closest][0]
                    GIFTS[gid]["telegram_gift_id"] = tg_gift.id
                    GIFTS[gid]["star_cost"] = closest
                    print(f"   ⚠️ {gdata['title']} ({our_cost}⭐ → {closest}⭐) → {tg_gift.id}")
                else:
                    print(f"   ❌ {gdata['title']} — нет подарков!")
                    
    except Exception as e:
        print(f"❌ TG Gifts ошибка: {e}")
        import traceback
        traceback.print_exc()

@router.message(Command("tggifts"))
async def cmd_tggifts(message: Message):
    """Показать Telegram подарки"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        gifts = await bot.get_available_gifts()
        
        # Группируем по цене
        by_price = {}
        for g in gifts.gifts:
            if g.star_count not in by_price:
                by_price[g.star_count] = []
            by_price[g.star_count].append(g.id)
        
        text = f"🎁 **Telegram Gifts ({len(gifts.gifts)}):**\n\n"
        
        for price in sorted(by_price.keys()):
            ids = by_price[price]
            text += f"**{price}⭐:** {len(ids)} шт.\n"
            for gid in ids[:3]:  # Показываем первые 3
                text += f"  `{gid}`\n"
            if len(ids) > 3:
                text += f"  _...и ещё {len(ids)-3}_\n"
        
        text += f"\n**Наши подарки:**\n"
        for gid, gdata in GIFTS.items():
            tg_id = gdata.get('telegram_gift_id')
            cost = gdata.get('star_cost')
            if tg_id:
                text += f"✅ {gdata['title']} ({cost}⭐)\n"
            else:
                text += f"❌ {gdata['title']} ({cost}⭐) — НЕ ЗАМАПЛЕН\n"
        
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> bool:
    gift = GIFTS.get(gift_id)
    if not gift:
        return False

    tg_id = gift.get("telegram_gift_id")
    if tg_id:
        try:
            await bot.send_gift(user_id=user_id, gift_id=tg_id, text=text or gift["title"])
            print(f"✅ Gift {gift['title']} → {user_id}")
            return True
        except Exception as e:
            print(f"❌ sendGift: {e}")

    try:
        await bot.send_animation(chat_id=user_id, animation=gift["gif_url"], caption=f"🎁 {gift['title']}\n\n{text or ''}")
        return True
    except:
        return False


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
        "👋 **Привет! Это Подарочница!**\n\n"
        "🎁 Подарки за ⭐ Stars\n"
        "🎰 Кейсы с призами\n"
        "🎟 Промокоды\n\n"
        "/promocode КОД — активировать\n"
        "/mycredits — кредиты\n"
        "/d сумма — поддержать бота\n"
        "/myid — узнать ID",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    is_admin = "✅ Админ" if message.from_user.id in ADMIN_IDS else "❌ Не админ"
    sheets = "✅" if spreadsheet else "❌"
    await message.answer(
        f"👤 ID: `{message.from_user.id}`\n🔐 {is_admin}\n📊 Sheets: {sheets}",
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("mycredits"))
async def cmd_mycredits(message: Message):
    credits = get_user_credits(message.from_user.id)
    text = "💳 **Кредиты:**\n\n"
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
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# ===== ДОНАТ =====
@router.message(Command("d"))
async def cmd_donate(message: Message, command: CommandObject):
    """Донат боту"""
    if not command.args:
        await message.answer(
            "💝 **Поддержать бота:**\n\n"
            "`/d 10` — задонатить 10 ⭐\n"
            "`/d 50` — задонатить 50 ⭐\n"
            "`/d 100` — задонатить 100 ⭐\n\n"
            "Минимум: 1 ⭐",
            parse_mode=ParseMode.MARKDOWN
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
        await message.answer("❌ Укажи число: `/d 10`", parse_mode=ParseMode.MARKDOWN)
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
            f"💝 **Донат {amount} ⭐**\n\nСпасибо за поддержку!",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        print(f"❌ Donate error: {e}")
        await message.answer("❌ Ошибка создания платежа")


# ===== ПРОМОКОД ДЛЯ ЮЗЕРОВ =====
@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    uid = message.from_user.id
    username = message.from_user.username
    print(f"🎟 /promocode от {uid}, args: '{command.args}'")

    if not command.args:
        await message.answer("❌ Напиши: `/promocode КОД`", parse_mode=ParseMode.MARKDOWN)
        return

    code = command.args.strip().upper()

    try:
        promocodes = get_promocodes()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await message.answer("❌ Ошибка базы.")
        return

    print(f"   Код: {code}, в базе: {list(promocodes.keys())}")

    if code not in promocodes:
        await message.answer(f"❌ Промокод `{code}` не найден!", parse_mode=ParseMode.MARKDOWN)
        return

    promo = promocodes[code]

    if uid in promo.get("used_by", []):
        await message.answer("⚠️ Уже использован!")
        return

    if promo.get("uses", 0) >= promo.get("max_uses", 0):
        await message.answer("❌ Закончился!")
        return

    rt, ri = promo["reward_type"], promo["reward_id"]

    # Проверяем, оплачен ли промокод (для подарков)
    if rt == "gift":
        if not promo.get("paid", False):
            await message.answer("❌ Этот промокод ещё не оплачен создателем!")
            return
        
        # Отправляем реальный подарок!
        gift = GIFTS.get(ri)
        if gift:
            text = f"🎟 Промокод {code}"
            success = await send_real_gift(uid, ri, text)
            
            if success:
                promo["uses"] = promo.get("uses", 0) + 1
                promo.setdefault("used_by", []).append(uid)
                save_promocode(code, promo)
                
                await message.answer(
                    f"✅ **Промокод активирован!**\n\n"
                    f"🎁 Тебе отправлен: {gift['title']}!",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await message.answer("❌ Ошибка отправки подарка. Попробуй позже.")
            return
    
    # Для кейсов — выдаём кредит
    try:
        add_user_credit(uid, rt, ri)
    except Exception as e:
        print(f"❌ Кредит: {e}")
        await message.answer("❌ Ошибка выдачи.")
        return

    promo["uses"] = promo.get("uses", 0) + 1
    promo.setdefault("used_by", []).append(uid)

    try:
        save_promocode(code, promo)
    except Exception as e:
        print(f"❌ Сохранение: {e}")

    title = CASES.get(ri, {}).get("title", ri) if rt == "case" else GIFTS.get(ri, {}).get("title", ri)
    await message.answer(f"✅ **Активировано!**\n\n🎁 Получено: {title}", parse_mode=ParseMode.MARKDOWN)


# ===== АДМИН КОМАНДЫ =====
@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer(f"⛔ Не админ!\nID: `{uid}`", parse_mode=ParseMode.MARKDOWN)
        return

    if not command.args:
        await message.answer(
            "📝 **Промокоды:**\n\n"
            "**Создать (с оплатой):**\n"
            "`/pr new КОД тип:id кол-во`\n\n"
            "**Примеры:**\n"
            "`/pr new FREEBEAR gift:bear 5`\n"
            "→ Оплатишь 75⭐ (15⭐ × 5), юзеры получат мишек\n\n"
            "`/pr new FREECASE case:premium 10`\n"
            "→ Бесплатно, юзеры получат кредиты на кейс\n\n"
            "**Типы:**\n"
            "🎁 `gift:` rocket/rose/box/heart/bear\n"
            "📦 `case:` premium/rich/ultra\n\n"
            "`/pr list` — список\n"
            "`/pr delete КОД`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    args = command.args.split()
    action = args[0].lower()

    if action == "new" and len(args) >= 4:
        code = args[1].upper()
        try:
            reward_type, reward_id = args[2].split(":", 1)
        except:
            await message.answer("❌ Формат: `тип:id`", parse_mode=ParseMode.MARKDOWN)
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
            await message.answer(f"❌ Нет кейса `{reward_id}`", parse_mode=ParseMode.MARKDOWN)
            return

        if reward_type == "gift" and reward_id not in GIFTS:
            await message.answer(f"❌ Нет подарка `{reward_id}`", parse_mode=ParseMode.MARKDOWN)
            return

        # Для подарков — нужна оплата!
        if reward_type == "gift":
            gift = GIFTS[reward_id]
            total_cost = gift["star_cost"] * max_uses
            
            # Сохраняем pending промокод
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
                    f"🎟 **Промокод {code}**\n\n"
                    f"🎁 {max_uses}× {gift['title']}\n"
                    f"💰 Стоимость: {total_cost} ⭐\n\n"
                    f"Оплати чтобы активировать промокод:",
                    reply_markup=kb,
                    parse_mode=ParseMode.MARKDOWN
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
            "paid": True  # Кейсы не требуют оплаты
        }

        save_promocode(code, promo)

        title = CASES.get(reward_id, {}).get("title", reward_id)
        await message.answer(
            f"✅ **Создан!**\n\n🎟 `{code}` → {title} (x{max_uses})\n\nЮзеры: `/promocode {code}`",
            parse_mode=ParseMode.MARKDOWN
        )

    elif action == "list":
        promocodes = get_promocodes()
        if not promocodes:
            await message.answer("📭 Пусто")
            return
        text = "📋 **Промокоды:**\n\n"
        for c, p in promocodes.items():
            paid_status = "✅" if p.get("paid", False) else "❌ не оплачен"
            text += f"`{c}` — {p.get('uses', 0)}/{p.get('max_uses', 0)} {paid_status}\n"
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)

    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        delete_promocode(code)
        await message.answer(f"✅ `{code}` удалён", parse_mode=ParseMode.MARKDOWN)

    else:
        await message.answer("❌ Напиши `/pr`", parse_mode=ParseMode.MARKDOWN)


@router.message(Command("addadmin"))
async def cmd_addadmin(message: Message, command: CommandObject):
    """Добавить админа (только для главного админа)"""
    uid = message.from_user.id
    
    # Только первый админ в списке может добавлять других
    if not ADMIN_IDS or uid != ADMIN_IDS[0]:
        await message.answer("⛔ Только главный админ может добавлять других!")
        return
    
    if not command.args:
        await message.answer(
            "👥 **Добавить админа:**\n\n"
            "`/addadmin 123456789`\n\n"
            f"Текущие админы: `{ADMIN_IDS}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        new_admin_id = int(command.args.strip())
    except ValueError:
        await message.answer("❌ Укажи ID числом!")
        return
    
    if new_admin_id in ADMIN_IDS:
        await message.answer("⚠️ Уже админ!")
        return
    
    ADMIN_IDS.append(new_admin_id)
    await message.answer(
        f"✅ Админ `{new_admin_id}` добавлен!\n\n"
        f"Текущие: `{ADMIN_IDS}`\n\n"
        f"⚠️ Чтобы сохранить навсегда — добавь в ADMIN_IDS на Render!",
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    sheets = "✅" if spreadsheet else "❌"
    promos = len(get_promocodes())
    await message.answer(
        f"🏓 Pong!\n"
        f"📊 Sheets: {sheets}\n"
        f"🎟 Промо: {promos}\n"
        f"🎁 TG: {len(available_telegram_gifts)}\n"
        f"👥 Админы: {len(ADMIN_IDS)}"
    )


@router.message(Command("debug"))
async def cmd_debug(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    promocodes = get_promocodes()
    credits = get_user_credits(message.from_user.id)
    await message.answer(
        f"🔧 **Debug:**\n\n"
        f"WEBAPP: `{WEBAPP_URL}`\n"
        f"ADMINS: `{ADMIN_IDS}`\n"
        f"Sheets: {'✅' if spreadsheet else '❌'}\n"
        f"Промо: {len(promocodes)}\n"
        f"Pending: {list(MEMORY.get('pending_promocodes', {}).keys())}\n"
        f"TG Gifts: {len(available_telegram_gifts)}",
        parse_mode=ParseMode.MARKDOWN
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
    
    print(f"💰 {buyer_id} (@{buyer_username}): {item_type}, {total}⭐")

    try:
        # ===== ДОНАТ =====
        if item_type == "donate":
            save_donation(buyer_id, buyer_username, total)
            await message.answer(
                f"💝 **Спасибо за донат {total} ⭐!**\n\n"
                f"Ты лучший! 🙏",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Уведомляем админов
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"💝 Новый донат!\n\n"
                        f"👤 @{buyer_username or buyer_id}\n"
                        f"⭐ {total} Stars"
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
                    f"✅ **Промокод `{code}` оплачен!**\n\n"
                    f"🎁 {pending['max_uses']}× {gift.get('title', pending['reward_id'])}\n\n"
                    f"Раздавай: `/promocode {code}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await message.answer("✅ Оплата прошла, но промокод не найден. Обратись к админу.")
            return

        # ===== ПОКУПКА ПОДАРКА =====
        if item_type == "gift":
            item_id = payload.get("id")
            sender_key = payload.get("sender")
            
            gift = GIFTS[item_id]
            sender_text = format_gift_text(sender_key, buyer_username)
            
            await send_real_gift(buyer_id, item_id, sender_text)
            save_purchase(buyer_id, {
                "type": "gift",
                "gift_id": item_id,
                "paid": total,
                "sender": sender_key or ""
            })
            
            msg = f"🎉 {gift['title']} отправлен!"
            if sender_text:
                msg += f"\n📝 {sender_text}"
            await message.answer(msg)
            return

        # ===== ПОКУПКА КЕЙСА =====
        if item_type == "case":
            item_id = payload.get("id")
            case = CASES[item_id]
            won = roll_case(item_id)

            if won and won != "nothing":
                wg = GIFTS[won]
                case_text = f"Для @{buyer_username} из {case['title']}" if buyer_username else None
                
                await send_real_gift(buyer_id, won, case_text)
                save_purchase(buyer_id, {
                    "type": "case_win",
                    "case_id": item_id,
                    "gift_id": won,
                    "paid": total
                })
                await message.answer(
                    f"🎰 **{case['title']}**\n\n🎉 {wg['title']}!",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                save_purchase(buyer_id, {"type": "case_lose", "case_id": item_id, "paid": total})
                await message.answer(
                    f"🎰 **{case['title']}**\n\n😔 Ничего...",
                    parse_mode=ParseMode.MARKDOWN
                )
            return
            
    except Exception as e:
        print(f"❌ Payment error: {e}")
        await message.answer("✅ Оплата прошла!")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"🔧 BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
    print(f"🔧 WEBAPP_URL: {WEBAPP_URL}")
    print(f"🔧 ADMIN_IDS: {ADMIN_IDS}")
    print(f"🔧 SELF_URL: {SELF_URL}")
    print(f"🔧 Sheets: {'✅' if GOOGLE_SHEET_ID else '❌'}")

    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()

    print("⏳ Ждём 5 сек...")
    await asyncio.sleep(5)

    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    print("✅ Бот работает!")
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
    return {"app": "Подарочница", "status": "running"}


@app.head("/health")
@app.get("/health")
async def health():
    return {"status": "ok", "sheets": bool(spreadsheet), "time": datetime.now().isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

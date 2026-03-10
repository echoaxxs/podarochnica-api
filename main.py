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
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

print(f"🔧 BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
print(f"🔧 WEBAPP_URL: {WEBAPP_URL}")
print(f"🔧 ADMIN_IDS: {ADMIN_IDS}")
print(f"🔧 SELF_URL: {SELF_URL}")
print(f"🔧 GOOGLE_SHEET_ID: {'✅' if GOOGLE_SHEET_ID else '❌'}")
print(f"🔧 GOOGLE_CREDENTIALS: {'✅' if GOOGLE_CREDENTIALS else '❌'}")

# ===== ПОДПИСИ =====
SENDERS = ["@echoaxxs", "@bogclm"]
SIGNATURE_COST = 1


def format_gift_text(sender_key: str, recipient_username: str = None) -> str:
    if not sender_key or sender_key not in SENDERS:
        return None
    
    if recipient_username:
        recipient = recipient_username.lstrip("@")
        return f"Для @{recipient} от {sender_key}"
    else:
        return f"От {sender_key}"

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
            {"gift_id": "rocket", "chance": 0.10},
            {"gift_id": "nothing", "chance": 0.20},
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
        
        # Создаём листы если их нет
        existing = [ws.title for ws in spreadsheet.worksheets()]
        
        if "promocodes" not in existing:
            ws = spreadsheet.add_worksheet("promocodes", rows=1000, cols=10)
            ws.append_row(["code", "reward_type", "reward_id", "max_uses", "uses", "used_by", "created"])
            print("  ✓ Создан лист promocodes")
        
        if "credits" not in existing:
            ws = spreadsheet.add_worksheet("credits", rows=5000, cols=5)
            ws.append_row(["user_id", "type", "item_id", "amount"])
            print("  ✓ Создан лист credits")
        
        if "purchases" not in existing:
            ws = spreadsheet.add_worksheet("purchases", rows=10000, cols=8)
            ws.append_row(["user_id", "type", "item_id", "paid", "sender", "timestamp"])
            print("  ✓ Создан лист purchases")
        
        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def get_sheet(name: str):
    """Получить лист по имени"""
    try:
        return spreadsheet.worksheet(name)
    except Exception as e:
        print(f"❌ Лист '{name}' не найден: {e}")
        return None


# ===== ПРОМОКОДЫ (Google Sheets) =====
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
            if used_by_str:
                try:
                    used_by = json.loads(used_by_str)
                except:
                    used_by = []
            else:
                used_by = []
            
            result[code] = {
                "reward_type": str(row.get("reward_type", "")),
                "reward_id": str(row.get("reward_id", "")),
                "max_uses": int(row.get("max_uses", 0)),
                "uses": int(row.get("uses", 0)),
                "used_by": used_by,
                "created": str(row.get("created", ""))
            }
        
        return result
    except Exception as e:
        print(f"❌ get_promocodes: {e}")
        return {}


def save_promocode(code: str, promo: dict):
    if not spreadsheet:
        MEMORY.setdefault("promocodes", {})[code] = promo
        return
    
    try:
        ws = get_sheet("promocodes")
        if not ws:
            return
        
        # Ищем строку с этим кодом
        all_values = ws.get_all_values()
        found_row = None
        
        for i, row in enumerate(all_values):
            if i == 0:
                continue  # Пропускаем заголовок
            if row and row[0] == code:
                found_row = i + 1  # gspread индексы с 1
                break
        
        row_data = [
            code,
            promo.get("reward_type", ""),
            promo.get("reward_id", ""),
            promo.get("max_uses", 0),
            promo.get("uses", 0),
            json.dumps(promo.get("used_by", [])),
            promo.get("created", "")
        ]
        
        if found_row:
            ws.update(f"A{found_row}:G{found_row}", [row_data])
        else:
            ws.append_row(row_data)
        
        print(f"   ✅ Промокод {code} сохранён")
    except Exception as e:
        print(f"❌ save_promocode: {e}")


def delete_promocode(code: str):
    if not spreadsheet:
        MEMORY.get("promocodes", {}).pop(code, None)
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
                print(f"   ✅ Промокод {code} удалён")
                return
    except Exception as e:
        print(f"❌ delete_promocode: {e}")


# ===== КРЕДИТЫ (Google Sheets) =====
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
        print(f"❌ get_user_credits: {e}")
        return {"cases": {}, "gifts": {}}


def save_user_credit(user_id: int, item_type: str, item_id: str, amount: int):
    if not spreadsheet:
        credits = MEMORY.setdefault("credits", {}).setdefault(str(user_id), {"cases": {}, "gifts": {}})
        cat = "cases" if item_type == "case" else "gifts"
        credits[cat][item_id] = amount
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
        
        print(f"   ✅ Кредит: {user_id} {item_type}:{item_id} = {amount}")
    except Exception as e:
        print(f"❌ save_user_credit: {e}")


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


# ===== ПОКУПКИ (Google Sheets) =====
def save_purchase(user_id: int, data: dict):
    if not spreadsheet:
        MEMORY.setdefault("purchases", {}).setdefault(str(user_id), []).append({**data, "timestamp": datetime.now().isoformat()})
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
        print(f"❌ save_purchase: {e}")


# Память как фоллбэк
MEMORY = {}


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
        print(f"📦 Загружено {len(gifts.gifts)} Telegram подарков")
        for gift in gifts.gifts:
            available_telegram_gifts[gift.star_count] = gift
        
        for gid, gdata in GIFTS.items():
            cost = gdata["star_cost"]
            if cost in available_telegram_gifts:
                GIFTS[gid]["telegram_gift_id"] = available_telegram_gifts[cost].id
                print(f"  ✓ {gdata['title']} → TG Gift")
    except Exception as e:
        print(f"❌ TG Gifts: {e}")


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> bool:
    gift = GIFTS.get(gift_id)
    if not gift:
        return False
    
    tg_id = gift.get("telegram_gift_id")
    if tg_id:
        try:
            await bot.send_gift(user_id=user_id, gift_id=tg_id, text=text or gift["title"])
            print(f"✅ Gift → {user_id}")
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
        "/promocode КОД\n"
        "/mycredits\n"
        "/myid",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    is_admin = "✅ Админ" if message.from_user.id in ADMIN_IDS else "❌ Не админ"
    sheets = "✅ Подключён" if spreadsheet else "❌ Нет"
    await message.answer(
        f"👤 ID: `{message.from_user.id}`\n"
        f"🔐 {is_admin}\n"
        f"📊 Google Sheets: {sheets}",
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


@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    uid = message.from_user.id
    print(f"🎟 /promocode от {uid}, args: '{command.args}'")
    
    if not command.args:
        await message.answer("❌ Напиши: `/promocode КОД`", parse_mode=ParseMode.MARKDOWN)
        return
    
    code = command.args.strip().upper()
    
    try:
        promocodes = get_promocodes()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        await message.answer("❌ Ошибка чтения базы. Попробуй позже.")
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
    
    try:
        add_user_credit(uid, rt, ri)
    except Exception as e:
        print(f"❌ Ошибка кредита: {e}")
        await message.answer("❌ Ошибка выдачи. Попробуй позже.")
        return
    
    promo["uses"] = promo.get("uses", 0) + 1
    promo.setdefault("used_by", []).append(uid)
    
    try:
        save_promocode(code, promo)
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
    
    title = CASES.get(ri, {}).get("title", ri) if rt == "case" else GIFTS.get(ri, {}).get("title", ri)
    await message.answer(f"✅ **Активировано!**\n\n🎁 Получено: {title}", parse_mode=ParseMode.MARKDOWN)
    print(f"   ✅ Выдано: {title}")


@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    uid = message.from_user.id
    if uid not in ADMIN_IDS:
        await message.answer(f"⛔ Не админ!\nID: `{uid}`\nADMIN_IDS: `{ADMIN_IDS}`", parse_mode=ParseMode.MARKDOWN)
        return
    
    if not command.args:
        await message.answer(
            "📝 **Промокоды:**\n\n"
            "`/pr new КОД тип:id лимит`\n"
            "Пример: `/pr new LUCKY case:premium 100`\n\n"
            "`/pr list`\n`/pr delete КОД`",
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
            await message.answer("❌ Лимит = число!")
            return
        
        if reward_type not in ("case", "gift"):
            await message.answer("❌ Тип: case или gift")
            return
        
        promo = {
            "reward_type": reward_type,
            "reward_id": reward_id,
            "max_uses": max_uses,
            "uses": 0,
            "used_by": [],
            "created": datetime.now().isoformat()
        }
        
        try:
            save_promocode(code, promo)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
            return
        
        title = CASES.get(reward_id, {}).get("title", reward_id) if reward_type == "case" else GIFTS.get(reward_id, {}).get("title", reward_id)
        await message.answer(f"✅ `{code}` → {title} (x{max_uses})", parse_mode=ParseMode.MARKDOWN)
    
    elif action == "list":
        try:
            promocodes = get_promocodes()
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
            return
        
        if not promocodes:
            await message.answer("📭 Пусто")
            return
        
        text = "📋 **Промокоды:**\n\n"
        for c, p in promocodes.items():
            text += f"`{c}` — {p.get('uses',0)}/{p.get('max_uses',0)}\n"
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)
    
    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        try:
            delete_promocode(code)
            await message.answer(f"✅ `{code}` удалён", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
    
    else:
        await message.answer("❌ Напиши `/pr`", parse_mode=ParseMode.MARKDOWN)


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    sheets = "✅" if spreadsheet else "❌"
    promos = len(get_promocodes())
    await message.answer(f"🏓 Pong!\n📊 Sheets: {sheets}\n🎟 Промо: {promos}")


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
    item_id = payload.get("id")
    sender_key = payload.get("sender")

    print(f"💰 {buyer_id} (@{buyer_username}): {item_type}:{item_id}, {total}⭐")

    try:
        if item_type == "gift":
            gift = GIFTS[item_id]
            
            # Исправлено: используем format_gift_text
            sender_text = format_gift_text(sender_key, buyer_username)
            
            await send_real_gift(buyer_id, item_id, sender_text)
            save_purchase(buyer_id, {
                "type": "gift",
                "gift_id": item_id,
                "paid": total,
                "sender": sender_key or ""
            })
            
            # Сообщение покупателю
            msg = f"🎉 {gift['title']} отправлен!"
            if sender_text:
                msg += f"\n📝 {sender_text}"
            await message.answer(msg)

        elif item_type == "case":
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
    except Exception as e:
        print(f"❌ Payment: {e}")
        await message.answer("✅ Оплата прошла!")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Запуск...")
    init_google_sheets()
    await load_telegram_gifts()
    asyncio.create_task(dp.start_polling(bot))
    asyncio.create_task(keep_alive())
    print("✅ Бот работает!")
    yield


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
            
            # Исправлено: проверка для list
            if req.sender and req.sender in SENDERS:
                desc = format_gift_text(req.sender, buyer_username) or gift["title"]
            else:
                desc = gift["title"]

            link = await bot.create_invoice_link(
                title=gift["title"],
                description=desc,
                payload=json.dumps({
                    "type": "gift",
                    "id": req.giftId,
                    "sender": req.sender
                }),
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


@app.get("/health")
async def health():
    return {"status": "ok", "sheets": bool(spreadsheet), "time": datetime.now().isoformat()}


@app.get("/")
async def root():
    return {"app": "Подарочница", "sheets": bool(spreadsheet)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

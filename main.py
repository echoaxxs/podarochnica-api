import os
import json
import hashlib
import hmac
import asyncio
import random
import httpx
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
REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
ORDERS_CHAT_ID = os.getenv("ORDERS_CHAT_ID", "")

# URL самого себя для пинга (Render даёт URL типа https://myapp.onrender.com)
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
PING_INTERVAL = 5 * 60  # 5 минут в секундах

# Подписи от которых можно отправлять
SENDERS = {
    "@echoaxxs": "С любовью от @echoaxxs 💜",
    "@bogclm": "Подарок от @bogclm ✨"
}

SIGNATURE_COST = 1

GIFTS = {
    "rocket": {
        "title": "🚀 Ракета",
        "price": 50,
        "star_cost": 50,
        "telegram_gift_id": None,
        "desc": "Улети к звёздам!",
        "gif_url": "https://podarochnica.pages.dev/rocket.gif",
    },
    "rose": {
        "title": "🌹 Роза",
        "price": 25,
        "star_cost": 25,
        "telegram_gift_id": None,
        "desc": "Прекрасная роза",
        "gif_url": "https://podarochnica.pages.dev/rose.gif",
    },
    "box": {
        "title": "🎁 Подарок",
        "price": 25,
        "star_cost": 25,
        "telegram_gift_id": None,
        "desc": "Сюрприз внутри",
        "gif_url": "https://podarochnica.pages.dev/gift.gif",
    },
    "heart": {
        "title": "❤️ Сердце",
        "price": 15,
        "star_cost": 15,
        "telegram_gift_id": None,
        "desc": "С любовью",
        "gif_url": "https://podarochnica.pages.dev/heart.gif",
    },
    "bear": {
        "title": "🧸 Мишка",
        "price": 15,
        "star_cost": 15,
        "telegram_gift_id": None,
        "desc": "Милый мишка",
        "gif_url": "https://podarochnica.pages.dev/bear.gif",
    },
}

CASES = {
    "premium": {
        "title": "💎 Премиум кейс",
        "price": 30,
        "drops": [
            {"gift_id": "rose", "chance": 0.35},
            {"gift_id": "box", "chance": 0.35},
            {"gift_id": "rocket", "chance": 0.10},
            {"gift_id": "nothing", "chance": 0.20},
        ]
    },
    "rich": {
        "title": "💰 Кейс Богач",
        "price": 100,
        "drops": [
            {"gift_id": "rocket", "chance": 0.30},
            {"gift_id": "rose", "chance": 0.25},
            {"gift_id": "box", "chance": 0.25},
            {"gift_id": "heart", "chance": 0.10},
            {"gift_id": "nothing", "chance": 0.10},
        ]
    },
    "ultra": {
        "title": "🔥 Ультра кейс",
        "price": 500,
        "drops": [
            {"gift_id": "rocket", "chance": 0.50},
            {"gift_id": "rose", "chance": 0.20},
            {"gift_id": "box", "chance": 0.20},
            {"gift_id": "heart", "chance": 0.05},
            {"gift_id": "nothing", "chance": 0.05},
        ]
    },
}


# ===== SELF-PING (АНТИ-СОН) =====
async def keep_alive():
    """
    Пингует сам себя каждые 5 минут чтобы Render не усыпил сервис
    """
    if not SELF_URL:
        print("⚠️ SELF_URL не задан, keep-alive отключён")
        print("   Задай переменную SELF_URL=https://твой-сервис.onrender.com")
        return
    
    ping_url = f"{SELF_URL}/health"
    print(f"🏓 Keep-alive запущен: пинг {ping_url} каждые {PING_INTERVAL // 60} мин")
    
    # Ждём 30 секунд перед первым пингом (даём серверу запуститься)
    await asyncio.sleep(30)
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(ping_url, timeout=10)
                print(f"🏓 Ping OK: {response.status_code} @ {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print(f"🏓 Ping failed: {e}")
            
            await asyncio.sleep(PING_INTERVAL)


# ===== REDIS =====
async def redis_get(key: str):
    if not REDIS_URL:
        return None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{REDIS_URL}/get/{key}",
                headers={"Authorization": f"Bearer {REDIS_TOKEN}"}
            )
            data = resp.json()
            if data.get("result"):
                return json.loads(data["result"])
            return None
    except Exception as e:
        print(f"Redis GET error: {e}")
        return None


async def redis_set(key: str, value):
    if not REDIS_URL:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{REDIS_URL}/set/{key}",
                headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
                json=json.dumps(value, ensure_ascii=False)
            )
    except Exception as e:
        print(f"Redis SET error: {e}")


async def redis_incr(key: str, amount: int = 1):
    if not REDIS_URL:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{REDIS_URL}/incrby/{key}/{amount}",
                headers={"Authorization": f"Bearer {REDIS_TOKEN}"}
            )
    except Exception as e:
        print(f"Redis INCR error: {e}")


# ===== ИСТОРИЯ И СТАТИСТИКА =====
async def save_purchase(user_id: int, purchase_data: dict):
    key = f"purchases:{user_id}"
    purchases = await redis_get(key) or []
    purchases.append({
        **purchase_data,
        "timestamp": datetime.now().isoformat()
    })
    await redis_set(key, purchases[-100:])
    await redis_incr("stats:total_purchases")
    await redis_incr("stats:total_stars_earned", purchase_data.get("profit", 0))


async def get_stats():
    total_purchases = await redis_get("stats:total_purchases") or 0
    total_stars = await redis_get("stats:total_stars_earned") or 0
    return {"total_purchases": total_purchases, "total_stars_earned": total_stars}


# ===== ПРОМОКОДЫ =====
async def get_promocodes():
    return await redis_get("promocodes") or {}


async def save_promocodes(data):
    await redis_set("promocodes", data)


# ===== КРЕДИТЫ =====
async def get_user_credits(user_id: int):
    data = await redis_get(f"credits:{user_id}")
    return data or {"cases": {}, "gifts": {}}


async def save_user_credits(user_id: int, credits):
    await redis_set(f"credits:{user_id}", credits)


async def add_user_credit(user_id: int, item_type: str, item_id: str, amount: int = 1):
    credits = await get_user_credits(user_id)
    category = "cases" if item_type == "case" else "gifts"
    if item_id not in credits[category]:
        credits[category][item_id] = 0
    credits[category][item_id] += amount
    await save_user_credits(user_id, credits)


async def use_user_credit(user_id: int, item_type: str, item_id: str):
    credits = await get_user_credits(user_id)
    category = "cases" if item_type == "case" else "gifts"
    if credits.get(category, {}).get(item_id, 0) <= 0:
        return False
    credits[category][item_id] -= 1
    await save_user_credits(user_id, credits)
    return True


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
        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(parsed.items())
        )
        secret_key = hmac.new(
            b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256
        ).digest()
        calculated_hash = hmac.new(
            secret_key, data_check_string.encode(), hashlib.sha256
        ).hexdigest()
        if calculated_hash == received_hash:
            if "user" in parsed:
                parsed["user"] = json.loads(parsed["user"])
            return parsed
        return None
    except Exception:
        return None


async def load_telegram_gifts():
    global available_telegram_gifts
    
    try:
        gifts = await bot.get_available_gifts()
        print(f"📦 Загружено {len(gifts.gifts)} Telegram подарков:")
        
        for gift in gifts.gifts:
            print(f"  - ID: {gift.id}, Stars: {gift.star_count}")
            available_telegram_gifts[gift.star_count] = gift
        
        for gift_id, gift_data in GIFTS.items():
            star_cost = gift_data["star_cost"]
            if star_cost in available_telegram_gifts:
                tg_gift = available_telegram_gifts[star_cost]
                GIFTS[gift_id]["telegram_gift_id"] = tg_gift.id
                print(f"  ✓ {gift_data['title']} → Telegram Gift {tg_gift.id}")
            else:
                closest = min(available_telegram_gifts.keys(), 
                             key=lambda x: abs(x - star_cost), 
                             default=None)
                if closest:
                    tg_gift = available_telegram_gifts[closest]
                    GIFTS[gift_id]["telegram_gift_id"] = tg_gift.id
                    GIFTS[gift_id]["star_cost"] = closest
                    print(f"  ~ {gift_data['title']} → Telegram Gift {tg_gift.id} ({closest}⭐)")
                    
    except Exception as e:
        print(f"❌ Ошибка загрузки Telegram Gifts: {e}")


async def send_real_gift(user_id: int, gift_id: str, sender_text: Optional[str] = None) -> bool:
    gift = GIFTS.get(gift_id)
    if not gift:
        return False
    
    telegram_gift_id = gift.get("telegram_gift_id")
    if not telegram_gift_id:
        try:
            await bot.send_animation(
                chat_id=user_id,
                animation=gift["gif_url"],
                caption=f"🎁 {gift['title']}\n\n{sender_text or ''}"
            )
            return True
        except Exception as e:
            print(f"Ошибка GIF: {e}")
            return False
    
    try:
        await bot.send_gift(
            user_id=user_id,
            gift_id=telegram_gift_id,
            text=sender_text or f"🎁 {gift['title']}",
        )
        print(f"✅ Подарок {gift['title']} → {user_id}")
        return True
    except Exception as e:
        print(f"❌ sendGift error: {e}")
        try:
            await bot.send_animation(
                chat_id=user_id,
                animation=gift["gif_url"],
                caption=f"🎁 {gift['title']}\n\n{sender_text or ''}"
            )
        except:
            pass
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
async def cmd_start(message: Message, command: CommandObject):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🎁 Открыть подарочницу",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )]
    ])
    await message.answer(
        "👋 **Добро пожаловать в Подарочницу!**\n\n"
        "🎁 Покупай подарки за ⭐ Stars\n"
        "🎰 Открывай кейсы\n"
        "🎟 Активируй промокоды\n\n"
        "/gifts — мои подарки\n"
        "/promocode <код> — промокод\n"
        "/mycredits — кредиты",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("gifts"))
async def cmd_gifts(message: Message):
    purchases = await redis_get(f"purchases:{message.from_user.id}") or []
    
    if not purchases:
        await message.answer("📭 Пока пусто. Купи подарок! 🎁")
        return
    
    text = "🎁 **История:**\n\n"
    for p in purchases[-10:]:
        gift = GIFTS.get(p.get("gift_id"), {})
        title = gift.get("title", "Подарок")
        date = p.get("timestamp", "")[:10]
        text += f"• {title} ({date})\n"
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    if not command.args:
        await message.answer("❌ Использование: /promocode <код>")
        return

    code = command.args.strip().upper()
    user_id = message.from_user.id
    promocodes = await get_promocodes()

    if code not in promocodes:
        await message.answer("❌ Промокод не найден!")
        return

    promo = promocodes[code]

    if user_id in promo.get("used_by", []):
        await message.answer("⚠️ Уже использован!")
        return

    if promo["uses"] >= promo["max_uses"]:
        await message.answer("❌ Промокод закончился!")
        return

    await add_user_credit(user_id, promo["reward_type"], promo["reward_id"])

    promo["uses"] += 1
    promo.setdefault("used_by", []).append(user_id)
    await save_promocodes(promocodes)

    if promo["reward_type"] == "case":
        item_title = CASES.get(promo["reward_id"], {}).get("title", promo["reward_id"])
    else:
        item_title = GIFTS.get(promo["reward_id"], {}).get("title", promo["reward_id"])

    await message.answer(f"✅ Получено: {item_title}")


@router.message(Command("mycredits"))
async def cmd_mycredits(message: Message):
    credits = await get_user_credits(message.from_user.id)
    text = "💳 **Кредиты:**\n\n"
    has_any = False

    for case_id, amount in credits.get("cases", {}).items():
        if amount > 0:
            title = CASES.get(case_id, {}).get("title", case_id)
            text += f"📦 {title}: {amount}\n"
            has_any = True

    for gift_id, amount in credits.get("gifts", {}).items():
        if amount > 0:
            title = GIFTS.get(gift_id, {}).get("title", gift_id)
            text += f"🎁 {title}: {amount}\n"
            has_any = True

    if not has_any:
        text += "Пусто!"

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        return

    if not command.args:
        await message.answer(
            "`/pr new <код> <тип:id> <лимит>`\n"
            "`/pr list` | `/pr delete <код>`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    args = command.args.split()
    action = args[0].lower()

    if action == "new" and len(args) >= 4:
        code = args[1].upper()
        reward_type, reward_id = args[2].split(":", 1)
        max_uses = int(args[3])

        promocodes = await get_promocodes()
        promocodes[code] = {
            "reward_type": reward_type,
            "reward_id": reward_id,
            "max_uses": max_uses,
            "uses": 0,
            "used_by": []
        }
        await save_promocodes(promocodes)
        await message.answer(f"✅ `{code}` создан", parse_mode=ParseMode.MARKDOWN)

    elif action == "list":
        promocodes = await get_promocodes()
        if not promocodes:
            await message.answer("📭 Пусто")
            return
        text = "".join(f"`{c}` — {p['uses']}/{p['max_uses']}\n" for c, p in promocodes.items())
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)

    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        promocodes = await get_promocodes()
        if code in promocodes:
            del promocodes[code]
            await save_promocodes(promocodes)
            await message.answer("✅ Удалён")


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    stats = await get_stats()
    await message.answer(
        f"📊 Покупок: {stats['total_purchases']}\n"
        f"⭐ Профит: {stats['total_stars_earned']}",
        parse_mode=ParseMode.MARKDOWN
    )


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    """Проверка что бот работает"""
    uptime_info = f"SELF_URL: {SELF_URL or 'не задан'}"
    await message.answer(f"🏓 Pong!\n{uptime_info}")


# ===== ОПЛАТА =====
@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    payment = message.successful_payment
    payload = json.loads(payment.invoice_payload)
    
    buyer_id = message.from_user.id
    buyer_name = message.from_user.full_name
    total_paid = payment.total_amount
    
    item_type = payload.get("type")
    item_id = payload.get("id")
    sender_key = payload.get("sender")
    
    try:
        if item_type == "gift":
            gift = GIFTS[item_id]
            star_cost = gift["star_cost"]
            
            if sender_key and sender_key in SENDERS:
                sender_text = SENDERS[sender_key]
            else:
                sender_text = None
            
            success = await send_real_gift(buyer_id, item_id, sender_text)
            profit = total_paid - star_cost
            
            await save_purchase(buyer_id, {
                "type": "gift",
                "gift_id": item_id,
                "paid": total_paid,
                "profit": profit
            })
            
            await message.answer(
                f"🎉 {gift['title']} отправлен!\n"
                f"{'📝 ' + sender_text if sender_text else ''}"
            )
            
            if ORDERS_CHAT_ID:
                try:
                    await bot.send_message(
                        ORDERS_CHAT_ID,
                        f"💰 {buyer_name}\n🎁 {gift['title']}\n⭐ {total_paid} (профит: {profit})"
                    )
                except:
                    pass

        elif item_type == "case":
            case = CASES[item_id]
            won_gift_id = roll_case(item_id)
            
            if won_gift_id and won_gift_id != "nothing":
                won_gift = GIFTS[won_gift_id]
                gift_cost = won_gift["star_cost"]
                profit = total_paid - gift_cost
                
                success = await send_real_gift(
                    buyer_id, 
                    won_gift_id, 
                    f"🎰 Из {case['title']}!"
                )
                
                await save_purchase(buyer_id, {
                    "type": "case_win",
                    "case_id": item_id,
                    "gift_id": won_gift_id,
                    "paid": total_paid,
                    "profit": profit
                })
                
                await message.answer(
                    f"🎰 **{case['title']}**\n\n"
                    f"🎉 Выпало: {won_gift['title']}!",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                profit = total_paid
                
                await save_purchase(buyer_id, {
                    "type": "case_lose",
                    "case_id": item_id,
                    "paid": total_paid,
                    "profit": profit
                })
                
                await message.answer(
                    f"🎰 **{case['title']}**\n\n"
                    f"😔 Ничего не выпало...",
                    parse_mode=ParseMode.MARKDOWN
                )
                
    except Exception as e:
        print(f"Payment error: {e}")
        await message.answer("✅ Оплата прошла!")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Загружаем подарки
    await load_telegram_gifts()
    
    # Запускаем бота
    asyncio.create_task(dp.start_polling(bot))
    
    # Запускаем keep-alive пинг
    asyncio.create_task(keep_alive())
    
    print("🚀 Бот запущен!")
    yield
    print("👋 Бот остановлен")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


class OpenCaseReq(BaseModel):
    initData: str
    caseId: str


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    try:
        if req.giftId and req.giftId in GIFTS:
            gift = GIFTS[req.giftId]
            price = gift["price"] + (SIGNATURE_COST if req.sender else 0)
            
            desc = gift["desc"]
            if req.sender and req.sender in SENDERS:
                desc += f"\n{SENDERS[req.sender]}"
            
            payload = json.dumps({
                "type": "gift",
                "id": req.giftId,
                "sender": req.sender
            })

            link = await bot.create_invoice_link(
                title=gift["title"],
                description=desc,
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=gift["title"], amount=price)]
            )
            return {"link": link}

        elif req.caseId and req.caseId in CASES:
            case = CASES[req.caseId]
            
            payload = json.dumps({
                "type": "case",
                "id": req.caseId
            })

            link = await bot.create_invoice_link(
                title=case["title"],
                description="Открой — выиграй подарок!",
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=case["title"], amount=case["price"])]
            )
            return {"link": link}

        raise HTTPException(status_code=400, detail="Item not found")

    except Exception as e:
        print(f"Invoice error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/get-credits")
async def get_credits(req: CreditsReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    user_id = auth_data["user"]["id"]
    return await get_user_credits(user_id)


@app.post("/api/use-credit")
async def use_credit_endpoint(req: UseCreditReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    user_id = auth_data["user"]["id"]
    success = await use_user_credit(user_id, req.itemType, req.itemId)

    if success:
        return {"success": True}
    raise HTTPException(status_code=400, detail="No credits")


@app.post("/api/open-case")
async def open_case_endpoint(req: OpenCaseReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    user_id = auth_data["user"]["id"]
    case_id = req.caseId
    
    if case_id not in CASES:
        raise HTTPException(status_code=400, detail="Case not found")
    
    won_gift_id = roll_case(case_id)
    
    if won_gift_id and won_gift_id != "nothing":
        won_gift = GIFTS[won_gift_id]
        
        await send_real_gift(
            user_id, 
            won_gift_id, 
            f"🎰 Из {CASES[case_id]['title']}!"
        )
        
        await save_purchase(user_id, {
            "type": "case_win_credit",
            "case_id": case_id,
            "gift_id": won_gift_id
        })
        
        return {
            "result": "win",
            "gift_id": won_gift_id,
            "gift_title": won_gift["title"],
            "gift_url": won_gift["gif_url"]
        }
    else:
        await save_purchase(user_id, {
            "type": "case_lose_credit",
            "case_id": case_id
        })
        return {"result": "nothing"}


@app.get("/health")
async def health():
    """Эндпоинт для проверки здоровья и self-ping"""
    return {
        "status": "ok",
        "time": datetime.now().isoformat(),
        "gifts_loaded": len(available_telegram_gifts),
        "uptime": "alive"
    }


@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {"message": "Подарочница API", "docs": "/docs"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

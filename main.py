import os
import json
import hashlib
import hmac
import asyncio
import httpx
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

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

GIFTS = {
    "rocket": {"title": "🚀 Ракета", "price": 50, "desc": "С космоса"},
    "rose":   {"title": "🌹 Роза",   "price": 25, "desc": "Прекрасная роза"},
    "box":    {"title": "🎁 Подарок", "price": 25, "desc": "Сюрприз внутри"},
    "heart":  {"title": "❤️ Сердце",  "price": 15, "desc": "С любовью"},
    "bear":   {"title": "🧸 Мишка",   "price": 15, "desc": "Милота"},
}

CASES = {
    "premium": {"title": "💎 Премиум кейс", "price": 30},
    "rich":    {"title": "💰 Кейс Богач",   "price": 100},
    "ultra":   {"title": "🔥 Ультра кейс",  "price": 500},
}

SIGNATURE_COST = 1


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


# ===== ПРОМОКОДЫ =====
async def get_promocodes():
    data = await redis_get("promocodes")
    return data or {}


async def save_promocodes(data):
    await redis_set("promocodes", data)


# ===== КРЕДИТЫ =====
async def get_user_credits(user_id: int):
    data = await redis_get(f"credits:{user_id}")
    if not data:
        return {"cases": {}, "gifts": {}}
    return data


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
    if item_id not in credits.get(category, {}):
        return False
    if credits[category][item_id] <= 0:
        return False
    credits[category][item_id] -= 1
    await save_user_credits(user_id, credits)
    return True


# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()


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


# ===== КОМАНДЫ =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🎁 Открыть подарочницу",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )]
    ])
    await message.answer(
        "👋 Привет! Это бот-подарочница.\n\n"
        "🎁 Жми кнопку — открыть каталог!\n"
        "🎟 /promocode <код> — активировать промокод\n"
        "💳 /mycredits — мои кредиты",
        reply_markup=kb
    )


@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    if not command.args:
        await message.answer(
            "❌ Использование: /promocode <код>\n"
            "Пример: /promocode LUCKY2024"
        )
        return

    code = command.args.strip().upper()
    user_id = message.from_user.id
    promocodes = await get_promocodes()

    if code not in promocodes:
        await message.answer("❌ Промокод не найден!")
        return

    promo = promocodes[code]

    if user_id in promo.get("used_by", []):
        await message.answer("⚠️ Ты уже использовал этот промокод!")
        return

    if promo["uses"] >= promo["max_uses"]:
        await message.answer("❌ Промокод закончился!")
        return

    reward_type = promo["reward_type"]
    reward_id = promo["reward_id"]

    await add_user_credit(user_id, reward_type, reward_id)

    promo["uses"] += 1
    if "used_by" not in promo:
        promo["used_by"] = []
    promo["used_by"].append(user_id)
    await save_promocodes(promocodes)

    if reward_type == "case":
        item_title = CASES.get(reward_id, {}).get("title", reward_id)
    else:
        item_title = GIFTS.get(reward_id, {}).get("title", reward_id)

    await message.answer(
        f"✅ Промокод активирован!\n\n"
        f"🎁 Ты получил: {item_title}\n\n"
        f"Открой WebApp чтобы использовать!"
    )


@router.message(Command("mycredits"))
async def cmd_mycredits(message: Message):
    credits = await get_user_credits(message.from_user.id)
    text = "💳 **Твои кредиты:**\n\n"
    has_any = False

    for case_id, amount in credits.get("cases", {}).items():
        if amount > 0:
            title = CASES.get(case_id, {}).get("title", case_id)
            text += f"📦 {title}: {amount} шт.\n"
            has_any = True

    for gift_id, amount in credits.get("gifts", {}).items():
        if amount > 0:
            title = GIFTS.get(gift_id, {}).get("title", gift_id)
            text += f"🎁 {title}: {amount} шт.\n"
            has_any = True

    if not has_any:
        text += "Пусто! Активируй промокод: /promocode <код>"

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# ===== АДМИН =====
@router.message(Command("pr"))
async def cmd_pr(message: Message, command: CommandObject):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Только для админов!")
        return

    if not command.args:
        await message.answer(
            "📝 **Управление промокодами:**\n\n"
            "`/pr new <код> <тип:id> <лимит>`\n"
            "Пример: `/pr new LUCKY case:premium 100`\n"
            "Пример: `/pr new GIFT2024 gift:rocket 50`\n\n"
            "`/pr list` — список\n"
            "`/pr delete <код>` — удалить\n\n"
            "**Кейсы:** premium, rich, ultra\n"
            "**Подарки:** rocket, rose, box, heart, bear",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    args = command.args.split()
    action = args[0].lower()

    if action == "new" and len(args) >= 4:
        code = args[1].upper()
        reward = args[2]
        try:
            max_uses = int(args[3])
        except ValueError:
            await message.answer("❌ Лимит должен быть числом!")
            return

        if ":" not in reward:
            await message.answer("❌ Формат: тип:id\nПример: case:premium")
            return

        reward_type, reward_id = reward.split(":", 1)

        if reward_type == "case" and reward_id not in CASES:
            await message.answer(
                f"❌ Кейс '{reward_id}' не найден!\n"
                f"Доступные: {', '.join(CASES.keys())}"
            )
            return

        if reward_type == "gift" and reward_id not in GIFTS:
            await message.answer(
                f"❌ Подарок '{reward_id}' не найден!\n"
                f"Доступные: {', '.join(GIFTS.keys())}"
            )
            return

        if reward_type not in ("case", "gift"):
            await message.answer("❌ Тип должен быть case или gift")
            return

        promocodes = await get_promocodes()
        promocodes[code] = {
            "reward_type": reward_type,
            "reward_id": reward_id,
            "max_uses": max_uses,
            "uses": 0,
            "used_by": [],
            "created": datetime.now().isoformat()
        }
        await save_promocodes(promocodes)

        if reward_type == "case":
            item_title = CASES[reward_id]["title"]
        else:
            item_title = GIFTS[reward_id]["title"]

        await message.answer(
            f"✅ Промокод создан!\n\n"
            f"🎟 Код: `{code}`\n"
            f"🎁 Награда: {item_title}\n"
            f"👥 Лимит: {max_uses} активаций",
            parse_mode=ParseMode.MARKDOWN
        )

    elif action == "list":
        promocodes = await get_promocodes()
        if not promocodes:
            await message.answer("📭 Промокодов пока нет")
            return

        text = "📋 **Список промокодов:**\n\n"
        for code, promo in promocodes.items():
            rt = promo["reward_type"]
            ri = promo["reward_id"]
            if rt == "case":
                item = CASES.get(ri, {}).get("title", ri)
            else:
                item = GIFTS.get(ri, {}).get("title", ri)
            text += (
                f"`{code}` → {item}\n"
                f"   Использовано: {promo['uses']}/{promo['max_uses']}\n\n"
            )

        await message.answer(text, parse_mode=ParseMode.MARKDOWN)

    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        promocodes = await get_promocodes()

        if code in promocodes:
            del promocodes[code]
            await save_promocodes(promocodes)
            await message.answer(
                f"✅ Промокод `{code}` удалён!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.answer("❌ Промокод не найден!")

    else:
        await message.answer("❌ Неверная команда. Напиши /pr для справки")


# ===== ОПЛАТА =====
@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    try:
        data = json.loads(payload)
        item_id = data.get("id")
        item_type = data.get("type")
        sender_name = data.get("sender")

        if item_type == "gift":
            title = GIFTS[item_id]["title"]
            text = f"🎉 Ты купил: {title}!"
            if sender_name:
                text += f"\nПодпись: от {sender_name}"
            await message.answer(text)

        elif item_type == "case":
            title = CASES[item_id]["title"]
            await message.answer(
                f"🎰 Ты оплатил: {title}!\n"
                f"Возвращайся в WebApp, чтобы открыть!"
            )
    except Exception:
        await message.answer("✅ Оплата прошла успешно!")


# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    dp.include_router(router)
    asyncio.create_task(dp.start_polling(bot))
    yield


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


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    try:
        if req.giftId and req.giftId in GIFTS:
            item = GIFTS[req.giftId]
            price = item["price"] + (SIGNATURE_COST if req.sender else 0)
            payload = json.dumps({
                "type": "gift",
                "id": req.giftId,
                "sender": req.sender
            })
            desc = item["desc"]
            if req.sender:
                desc += f"\nОт: {req.sender}"

            link = await bot.create_invoice_link(
                title=item["title"],
                description=desc,
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=item["title"], amount=price)]
            )
            return {"link": link}

        elif req.caseId and req.caseId in CASES:
            item = CASES[req.caseId]
            payload = json.dumps({
                "type": "case",
                "id": req.caseId
            })

            link = await bot.create_invoice_link(
                title=item["title"],
                description="Открытие платного кейса",
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=item["title"], amount=item["price"])]
            )
            return {"link": link}

        else:
            raise HTTPException(status_code=400, detail="Item not found")

    except Exception as e:
        print(f"Invoice error: {e}")
        raise HTTPException(status_code=500, detail="Server error")


@app.post("/api/get-credits")
async def get_credits(req: CreditsReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    user_id = auth_data["user"]["id"]
    credits = await get_user_credits(user_id)

    return {
        "cases": credits.get("cases", {}),
        "gifts": credits.get("gifts", {})
    }


@app.post("/api/use-credit")
async def use_credit_endpoint(req: UseCreditReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")

    user_id = auth_data["user"]["id"]
    success = await use_user_credit(user_id, req.itemType, req.itemId)

    if success:
        return {"success": True}
    else:
        raise HTTPException(status_code=400, detail="No credits")


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


# ===== ЗАПУСК =====
if __name__ == "__main__":
    dp.include_router(router)
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

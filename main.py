import os
import json
import hashlib
import hmac
import asyncio
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command
from aiogram.enums import ParseMode

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# ===== НАСТРОЙКИ =====
# Токен бота (берётся из переменных окружения сервера)
BOT_TOKEN = os.getenv("BOT_TOKEN", "ТВОЙ_ТОКЕН_ОТ_BOTFATHER")
# Ссылка на твой сайт с index.html
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://2977b3b5.podarochnica.pages.dev") 

# Конфиг цен (должен совпадать с JS)
GIFTS = {
    "rocket": {"title": "🚀 Ракета",   "price": 50, "desc": "С космоса"},
    "rose":   {"title": "🌹 Роза",     "price": 25, "desc": "Прекрасная роза"},
    "box":    {"title": "🎁 Подарок",  "price": 25, "desc": "Сюрприз внутри"},
    "heart":  {"title": "❤️ Сердце",   "price": 15, "desc": "С любовью"},
    "bear":   {"title": "🧸 Мишка",    "price": 15, "desc": "Милота"},
}
CASES = {
    "premium": {"title": "💎 Премиум кейс", "price": 30},
    "rich":    {"title": "💰 Кейс Богач",   "price": 100},
    "ultra":   {"title": "🔥 Ультра кейс",  "price": 500},
}
SIGNATURE_COST = 1

# ===== ИНИЦИАЛИЗАЦИЯ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

def validate_init_data(init_data: str) -> dict | None:
    """Проверка подлинности запроса от твоего WebApp"""
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        if "hash" not in parsed: return None
        received_hash = parsed.pop("hash")
        
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash == received_hash:
            if "user" in parsed: parsed["user"] = json.loads(parsed["user"])
            return parsed
        return None
    except Exception:
        return None

# ===== ЛОГИКА БОТА =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть подарочницу", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer("Привет! Жми кнопку ниже, чтобы открыть каталог:", reply_markup=kb)

@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True) # Разрешаем оплату звездами

@router.message(F.successful_payment)
async def successful_payment(message: Message):
    # Сюда прилетает ответ, когда юзер успешно оплатил инвойс
    payload = message.successful_payment.invoice_payload
    try:
        data = json.loads(payload)
        item_id = data.get("id")
        item_type = data.get("type")
        sender = data.get("sender")
        
        if item_type == "gift":
            title = GIFTS[item_id]["title"]
            text = f"🎉 Ты успешно купил: {title}!"
            if sender: text += f"\nПодпись: от {sender}"
            await message.answer(text)
            
        elif item_type == "case":
            title = CASES[item_id]["title"]
            await message.answer(f"🎰 Ты оплатил: {title}! Возвращайся в WebApp, чтобы открыть его.")
            
    except:
        await message.answer("✅ Оплата прошла успешно!")

# ===== ЛОГИКА API (FastAPI) =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запускаем бота параллельно с API сервером
    asyncio.create_task(dp.start_polling(bot))
    yield

app = FastAPI(lifespan=lifespan)

# Разрешаем твоему сайту делать запросы к этому API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Можно заменить на ["https://твой-сайт.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class InvoiceReq(BaseModel):
    initData: str
    giftId: str | None = None
    caseId: str | None = None
    sender: str | None = None

@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    # 1. Проверяем, что запрос реально из Телеграма
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")
    
    # 2. Формируем ссылку на оплату
    try:
        if req.giftId and req.giftId in GIFTS:
            item = GIFTS[req.giftId]
            price = item["price"] + (SIGNATURE_COST if req.sender else 0)
            payload = json.dumps({"type": "gift", "id": req.giftId, "sender": req.sender})
            
            link = await bot.create_invoice_link(
                title=item["title"],
                description=item["desc"] + (f"\nОт: {req.sender}" if req.sender else ""),
                payload=payload,
                currency="XTR", # XTR = Telegram Stars
                prices=[LabeledPrice(label=item["title"], amount=price)]
            )
            return {"link": link}
            
        elif req.caseId and req.caseId in CASES:
            item = CASES[req.caseId]
            payload = json.dumps({"type": "case", "id": req.caseId})
            
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
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Server error")

# ===== ЗАПУСК СЕРВЕРА =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

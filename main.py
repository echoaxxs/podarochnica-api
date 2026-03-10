import os
import json
import hashlib
import hmac
import asyncio
from datetime import datetime
from urllib.parse import parse_qsl
from contextlib import asynccontextmanager
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters import Command, CommandObject
from aiogram.enums import ParseMode

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
ADMIN_IDS = [123456789]  # <-- Замени на свой Telegram ID

# Файлы для хранения данных
PROMO_FILE = "promocodes.json"
CREDITS_FILE = "user_credits.json"

# Конфиг цен
GIFTS = {
    "rocket": {"title": "🚀 Ракета", "price": 50, "desc": "С космоса"},
    "rose": {"title": "🌹 Роза", "price": 25, "desc": "Прекрасная роза"},
    "box": {"title": "🎁 Подарок", "price": 25, "desc": "Сюрприз внутри"},
    "heart": {"title": "❤️ Сердце", "price": 15, "desc": "С любовью"},
    "bear": {"title": "🧸 Мишка", "price": 15, "desc": "Милота"},
}
CASES = {
    "premium": {"title": "💎 Премиум кейс", "price": 30},
    "rich": {"title": "💰 Кейс Богач", "price": 100},
    "ultra": {"title": "🔥 Ультра кейс", "price": 500},
}
SIGNATURE_COST = 1

# ===== РАБОТА С ДАННЫМИ =====
def load_json(filename: str) -> dict:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_json(filename: str, data: dict):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_promocodes() -> dict:
    return load_json(PROMO_FILE)

def save_promocodes(data: dict):
    save_json(PROMO_FILE, data)

def get_all_credits() -> dict:
    return load_json(CREDITS_FILE)

def save_all_credits(data: dict):
    save_json(CREDITS_FILE, data)

def get_user_credits(user_id: int) -> dict:
    """Получить кредиты пользователя"""
    all_credits = get_all_credits()
    user_key = str(user_id)
    if user_key not in all_credits:
        all_credits[user_key] = {"cases": {}, "gifts": {}}
        save_all_credits(all_credits)
    return all_credits[user_key]

def add_user_credit(user_id: int, item_type: str, item_id: str, amount: int = 1):
    """Добавить кредит пользователю"""
    all_credits = get_all_credits()
    user_key = str(user_id)
    
    if user_key not in all_credits:
        all_credits[user_key] = {"cases": {}, "gifts": {}}
    
    category = "cases" if item_type == "case" else "gifts"
    if item_id not in all_credits[user_key][category]:
        all_credits[user_key][category][item_id] = 0
    
    all_credits[user_key][category][item_id] += amount
    save_all_credits(all_credits)

def use_user_credit(user_id: int, item_type: str, item_id: str) -> bool:
    """Использовать кредит пользователя. Возвращает True если успешно"""
    all_credits = get_all_credits()
    user_key = str(user_id)
    category = "cases" if item_type == "case" else "gifts"
    
    if user_key not in all_credits:
        return False
    
    if item_id not in all_credits[user_key].get(category, {}):
        return False
    
    if all_credits[user_key][category][item_id] <= 0:
        return False
    
    all_credits[user_key][category][item_id] -= 1
    save_all_credits(all_credits)
    return True

# ===== ИНИЦИАЛИЗАЦИЯ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

def validate_init_data(init_data: str) -> dict | None:
    """Проверка подлинности запроса от WebApp"""
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
    except Exception:
        return None

# ===== КОМАНДЫ БОТА =====
@router.message(Command("start"))
async def cmd_start(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Открыть подарочницу", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])
    await message.answer(
        "👋 Привет! Это бот-подарочница.\n\n"
        "🎁 Жми кнопку ниже, чтобы открыть каталог!\n"
        "🎟 Используй /promocode <код> чтобы активировать промокод",
        reply_markup=kb
    )

@router.message(Command("promocode"))
async def cmd_promocode(message: Message, command: CommandObject):
    if not command.args:
        await message.answer("❌ Использование: /promocode <код>\n\nПример: /promocode LUCKY2024")
        return
    
    code = command.args.strip().upper()
    user_id = message.from_user.id
    promocodes = get_promocodes()
    
    if code not in promocodes:
        await message.answer("❌ Промокод не найден!")
        return
    
    promo = promocodes[code]
    
    # Проверяем, не использовал ли уже
    if user_id in promo.get("used_by", []):
        await message.answer("⚠️ Ты уже использовал этот промокод!")
        return
    
    # Проверяем лимит активаций
    if promo["uses"] >= promo["max_uses"]:
        await message.answer("❌ Промокод закончился!")
        return
    
    # Активируем промокод
    reward_type = promo["reward_type"]  # "case" или "gift"
    reward_id = promo["reward_id"]      # "premium", "rocket" и т.д.
    
    add_user_credit(user_id, reward_type, reward_id)
    
    # Обновляем промокод
    promo["uses"] += 1
    if "used_by" not in promo:
        promo["used_by"] = []
    promo["used_by"].append(user_id)
    save_promocodes(promocodes)
    
    # Формируем ответ
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
    """Показать кредиты пользователя"""
    credits = get_user_credits(message.from_user.id)
    
    text = "💳 **Твои кредиты:**\n\n"
    
    has_any = False
    
    # Кейсы
    if credits.get("cases"):
        for case_id, amount in credits["cases"].items():
            if amount > 0:
                title = CASES.get(case_id, {}).get("title", case_id)
                text += f"📦 {title}: {amount} шт.\n"
                has_any = True
    
    # Подарки
    if credits.get("gifts"):
        for gift_id, amount in credits["gifts"].items():
            if amount > 0:
                title = GIFTS.get(gift_id, {}).get("title", gift_id)
                text += f"🎁 {title}: {amount} шт.\n"
                has_any = True
    
    if not has_any:
        text += "Пусто! Активируй промокод с /promocode"
    
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)

# ===== АДМИН КОМАНДЫ =====
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
            "`/pr list` — список промокодов\n"
            "`/pr delete <код>` — удалить\n\n"
            "**Доступные типы:**\n"
            "📦 Кейсы: premium, rich, ultra\n"
            "🎁 Подарки: rocket, rose, box, heart, bear",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    args = command.args.split()
    action = args[0].lower()
    
    if action == "new" and len(args) >= 4:
        code = args[1].upper()
        reward = args[2]  # case:premium или gift:rocket
        max_uses = int(args[3])
        
        if ":" not in reward:
            await message.answer("❌ Формат награды: тип:id (например case:premium)")
            return
        
        reward_type, reward_id = reward.split(":", 1)
        
        # Проверяем валидность
        if reward_type == "case" and reward_id not in CASES:
            await message.answer(f"❌ Кейс '{reward_id}' не найден!\nДоступные: {', '.join(CASES.keys())}")
            return
        if reward_type == "gift" and reward_id not in GIFTS:
            await message.answer(f"❌ Подарок '{reward_id}' не найден!\nДоступные: {', '.join(GIFTS.keys())}")
            return
        
        promocodes = get_promocodes()
        promocodes[code] = {
            "reward_type": reward_type,
            "reward_id": reward_id,
            "max_uses": max_uses,
            "uses": 0,
            "used_by": [],
            "created": datetime.now().isoformat()
        }
        save_promocodes(promocodes)
        
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
        promocodes = get_promocodes()
        if not promocodes:
            await message.answer("📭 Промокодов нет")
            return
        
        text = "📋 **Список промокодов:**\n\n"
        for code, promo in promocodes.items():
            reward_type = promo["reward_type"]
            reward_id = promo["reward_id"]
            if reward_type == "case":
                item = CASES.get(reward_id, {}).get("title", reward_id)
            else:
                item = GIFTS.get(reward_id, {}).get("title", reward_id)
            
            text += f"`{code}` → {item}\n"
            text += f"   Использовано: {promo['uses']}/{promo['max_uses']}\n\n"
        
        await message.answer(text, parse_mode=ParseMode.MARKDOWN)
    
    elif action == "delete" and len(args) >= 2:
        code = args[1].upper()
        promocodes = get_promocodes()
        
        if code in promocodes:
            del promocodes[code]
            save_promocodes(promocodes)
            await message.answer(f"✅ Промокод `{code}` удалён!", parse_mode=ParseMode.MARKDOWN)
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
        sender = data.get("sender")
        
        if item_type == "gift":
            title = GIFTS[item_id]["title"]
            text = f"🎉 Ты успешно купил: {title}!"
            if sender:
                text += f"\nПодпись: от {sender}"
            await message.answer(text)
            
        elif item_type == "case":
            title = CASES[item_id]["title"]
            await message.answer(f"🎰 Ты оплатил: {title}! Возвращайся в WebApp!")
            
    except:
        await message.answer("✅ Оплата прошла успешно!")

# ===== API (FastAPI) =====
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
    itemType: str  # "case" или "gift"
    itemId: str    # "premium", "rocket" и т.д.

@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")
    
    try:
        if req.giftId and req.giftId in GIFTS:
            item = GIFTS[req.giftId]
            price = item["price"] + (SIGNATURE_COST if req.sender else 0)
            payload = json.dumps({"type": "gift", "id": req.giftId, "sender": req.sender})
            
            link = await bot.create_invoice_link(
                title=item["title"],
                description=item["desc"] + (f"\nОт: {req.sender}" if req.sender else ""),
                payload=payload,
                currency="XTR",
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

@app.post("/api/get-credits")
async def get_credits(req: CreditsReq):
    """Получить кредиты пользователя для отображения на сайте"""
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")
    
    user_id = auth_data["user"]["id"]
    credits = get_user_credits(user_id)
    
    return {
        "cases": credits.get("cases", {}),
        "gifts": credits.get("gifts", {})
    }

@app.post("/api/use-credit")
async def use_credit(req: UseCreditReq):
    """Использовать кредит (бесплатное открытие)"""
    auth_data = validate_init_data(req.initData)
    if not auth_data:
        raise HTTPException(status_code=401, detail="Invalid auth")
    
    user_id = auth_data["user"]["id"]
    
    success = use_user_credit(user_id, req.itemType, req.itemId)
    
    if success:
        return {"success": True, "message": "Credit used"}
    else:
        raise HTTPException(status_code=400, detail="No credits available")

# ===== ЗАПУСК =====
if __name__ == "__main__":
    dp.include_router(router)
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

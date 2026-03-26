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
print("🚀 ПОДАРОЧНИЦА v12.0 — NO SIGNATURES, DARK RED")
print("=" * 50)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://podarochnica.pages.dev")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("SELF_URL", ""))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

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

PITY_THRESHOLD = 20

# ===== ЗАХАРДКОЖЕННЫЕ ПОДАРКИ =====
HARDCODED_GIFTS = [
    {"id": "gift_15_1", "title": "Подарок", "emoji": "⭐", "price": 15, "telegram_gift_id": "5170145012310081615"},
    {"id": "gift_15_2", "title": "Подарок", "emoji": "⭐", "price": 15, "telegram_gift_id": "5170233102089322756"},
    {"id": "gift_25_1", "title": "Подарок", "emoji": "⭐", "price": 25, "telegram_gift_id": "5168103777563050263"},
    {"id": "gift_25_2", "title": "Подарок", "emoji": "⭐", "price": 25, "telegram_gift_id": "5170250947678437525"},
    {"id": "gift_50_1", "title": "Подарок", "emoji": "⭐", "price": 50, "telegram_gift_id": "5170144170496491616"},
    {"id": "gift_50_2", "title": "Подарок", "emoji": "⭐", "price": 50, "telegram_gift_id": "5170314324215857265"},
    {"id": "gift_50_3", "title": "Подарок", "emoji": "⭐", "price": 50, "telegram_gift_id": "5170564780938756245"},
    {"id": "gift_50_4", "title": "Подарок", "emoji": "⭐", "price": 50, "telegram_gift_id": "6028601630662853006"},
    {"id": "gift_100_1", "title": "Подарок", "emoji": "⭐", "price": 100, "telegram_gift_id": "5168043875654172773"},
    {"id": "gift_100_2", "title": "Подарок", "emoji": "⭐", "price": 100, "telegram_gift_id": "5170521118301225164"},
    {"id": "gift_100_3", "title": "Подарок", "emoji": "⭐", "price": 100, "telegram_gift_id": "5170690322832818290"},
]

PITY_REWARD_GIFT = "gift_15_1"
GIFTS = {}

# ===== КЕЙСЫ =====
CASES = {
    "mini": {"title": "🎲 Мини", "price": 1, "category": "cheap", "pity_enabled": True},
    "basic-5": {"title": "🎯 Базовый 5", "price": 5, "category": "cheap", "pity_enabled": True},
    "basic-10": {"title": "🎯 Базовый 10", "price": 10, "category": "cheap", "pity_enabled": True},
    "basic-15": {"title": "🎯 Базовый 15", "price": 15, "category": "cheap", "pity_enabled": True},
    "premium": {"title": "💎 Премиум", "price": 50, "category": "gifts", "win_chance": 0.30},
    "rich": {"title": "💰 Богач", "price": 100, "category": "gifts", "win_chance": 0.35},
    "star-100": {"title": "⭐ Star 100", "price": 100, "category": "stars", "type": "stars", "drops": [{"stars": 50, "chance": 0.70}, {"stars": 100, "chance": 0.25}, {"stars": 250, "chance": 0.05}]},
    "star-500": {"title": "⭐ Star 500", "price": 500, "category": "stars", "type": "stars", "drops": [{"stars": 250, "chance": 0.50}, {"stars": 500, "chance": 0.40}, {"stars": 1000, "chance": 0.10}]},
}

# ===== КЭШ =====
CACHE = {"settings": {}, "settings_time": 0, "balances": {}, "pity": {}, "promocodes": {}, "promocodes_time": 0}
CACHE_TTL = 30
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
            "promocodes": ["code", "reward_type", "reward_id", "max_uses", "uses", "used_by", "created", "signature_text"],
            "balances": ["user_id", "stars"],
            "pity": ["user_id", "spent"],
            "purchases": ["user_id", "type", "item_id", "paid", "timestamp"],
            "donations": ["user_id", "username", "amount", "timestamp"],
            "settings": ["key", "value"]
        }
        for name, headers in sheets.items():
            if name not in existing:
                ws = spreadsheet.add_worksheet(name, rows=1000, cols=10)
                ws.append_row(headers)
                if name == "settings":
                    ws.append_row(["maintenance", "FALSE"])
                    ws.append_row(["maintenance_text", "Идёт тех. перерыв."])
                    ws.append_row(["default_signature", ""])
        
        _load_all_cache()
        return True
    except Exception as e:
        print(f"❌ Google Sheets ошибка: {e}")
        return False


def _load_all_cache():
    try:
        _refresh_settings_cache()
        _refresh_balances_cache()
        _refresh_pity_cache()
    except Exception as e:
        print(f"⚠️ Ошибка кэша: {e}")


def get_sheet(name: str):
    try:
        return spreadsheet.worksheet(name) if spreadsheet else None
    except:
        return None


def _refresh_settings_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("settings")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["settings"] = {str(r.get("key", "")).strip().lower(): str(r.get("value", "")).strip() for r in rows if r.get("key")}
        CACHE["settings_time"] = time.time()
    except Exception as e:
        print(f"❌ settings: {e}")


def get_setting(key: str, default: str = "") -> str:
    if time.time() - CACHE.get("settings_time", 0) > CACHE_TTL:
        _refresh_settings_cache()
    return CACHE.get("settings", {}).get(key.lower(), default)


def set_setting(key: str, value: str):
    CACHE.setdefault("settings", {})[key.lower()] = value
    if not spreadsheet: return
    try:
        ws = get_sheet("settings")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0].lower() == key.lower():
                found = i + 1
                break
        if found:
            ws.update(f"B{found}", [[value]])
        else:
            ws.append_row([key, value])
    except Exception as e:
        print(f"❌ set_setting: {e}")


def is_maintenance_enabled() -> bool:
    return get_setting("maintenance", "FALSE").upper() in ("TRUE", "1", "YES")


def get_maintenance_text() -> str:
    return get_setting("maintenance_text", "Тех. перерыв")


def get_default_signature() -> str:
    return get_setting("default_signature", "")


def _refresh_balances_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("balances")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["balances"] = {str(r.get("user_id", "")): int(r.get("stars", 0)) for r in rows if r.get("user_id")}
    except Exception as e:
        print(f"❌ balances: {e}")


def _refresh_pity_cache():
    if not spreadsheet: return
    try:
        ws = get_sheet("pity")
        if not ws: return
        rows = ws.get_all_records()
        CACHE["pity"] = {str(r.get("user_id", "")): int(r.get("spent", 0)) for r in rows if r.get("user_id")}
    except Exception as e:
        print(f"❌ pity: {e}")


def get_star_balance(user_id: int) -> int:
    return CACHE.get("balances", {}).get(str(user_id), 0)


def set_star_balance(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("balances", {})[uid] = amount
    if not spreadsheet: return
    try:
        ws = get_sheet("balances")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == uid:
                found = i + 1
                break
        if found:
            ws.update(f"B{found}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_balance: {e}")


def add_star_balance(user_id: int, amount: int):
    set_star_balance(user_id, get_star_balance(user_id) + amount)


def use_star_balance(user_id: int, amount: int) -> bool:
    cur = get_star_balance(user_id)
    if cur < amount: return False
    set_star_balance(user_id, cur - amount)
    return True


def get_pity_spent(user_id: int) -> int:
    return CACHE.get("pity", {}).get(str(user_id), 0)


def set_pity_spent(user_id: int, amount: int):
    uid = str(user_id)
    CACHE.setdefault("pity", {})[uid] = amount
    if not spreadsheet: return
    try:
        ws = get_sheet("pity")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == uid:
                found = i + 1
                break
        if found:
            ws.update(f"B{found}", [[amount]])
        else:
            ws.append_row([uid, amount])
    except Exception as e:
        print(f"❌ set_pity: {e}")


def add_pity_spent(user_id: int, amount: int) -> int:
    new = get_pity_spent(user_id) + amount
    set_pity_spent(user_id, new)
    return new


def reset_pity(user_id: int):
    set_pity_spent(user_id, 0)


def get_promocodes() -> dict:
    now = time.time()
    if now - CACHE.get("promocodes_time", 0) < CACHE_TTL and CACHE.get("promocodes"):
        return CACHE["promocodes"]
    if not spreadsheet: return {}
    try:
        ws = get_sheet("promocodes")
        if not ws: return {}
        rows = ws.get_all_records()
        result = {}
        for r in rows:
            code = str(r.get("code", "")).strip()
            if not code: continue
            ub = str(r.get("used_by", ""))
            try: used_by = json.loads(ub) if ub else []
            except: used_by = []
            result[code] = {
                "reward_type": str(r.get("reward_type", "")),
                "reward_id": str(r.get("reward_id", "")),
                "max_uses": int(r.get("max_uses", 0)),
                "uses": int(r.get("uses", 0)),
                "used_by": [int(x) for x in used_by if str(x).isdigit()],
                "created": str(r.get("created", "")),
                "signature_text": str(r.get("signature_text", ""))
            }
        CACHE["promocodes"] = result
        CACHE["promocodes_time"] = now
        return result
    except Exception as e:
        print(f"❌ promocodes: {e}")
        return {}


def save_promocode(code: str, promo: dict):
    CACHE.setdefault("promocodes", {})[code] = promo
    CACHE["promocodes_time"] = time.time()
    if not spreadsheet: return
    try:
        ws = get_sheet("promocodes")
        if not ws: return
        all_vals = ws.get_all_values()
        found = None
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if row and row[0] == code:
                found = i + 1
                break
        data = [code, promo.get("reward_type", ""), promo.get("reward_id", ""), promo.get("max_uses", 0), promo.get("uses", 0), json.dumps(promo.get("used_by", [])), promo.get("created", ""), promo.get("signature_text", "")]
        if found:
            ws.update(f"A{found}:H{found}", [data])
        else:
            ws.append_row(data)
    except Exception as e:
        print(f"❌ save_promo: {e}")


def delete_promocode(code: str):
    CACHE.get("promocodes", {}).pop(code, None)
    if not spreadsheet: return
    try:
        ws = get_sheet("promocodes")
        if not ws: return
        for i, row in enumerate(ws.get_all_values()):
            if i == 0: continue
            if row and row[0] == code:
                ws.delete_rows(i + 1)
                return
    except Exception as e:
        print(f"❌ del_promo: {e}")


def save_purchase(user_id: int, data: dict):
    if not spreadsheet: return
    try:
        ws = get_sheet("purchases")
        if ws:
            ws.append_row([str(user_id), data.get("type", ""), data.get("gift_id", data.get("case_id", "")), data.get("paid", 0), datetime.now().isoformat()])
    except: pass


def save_donation(user_id: int, username: str, amount: int):
    if not spreadsheet: return
    try:
        ws = get_sheet("donations")
        if ws:
            ws.append_row([str(user_id), username or "", amount, datetime.now().isoformat()])
    except: pass


def save_pending_result(payment_id: str, result: dict):
    MEMORY["pending_results"][payment_id] = {**result, "ts": time.time()}


def get_pending_result(payment_id: str) -> Optional[dict]:
    return MEMORY.get("pending_results", {}).pop(payment_id, None)


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
        h = parsed.pop("hash")
        dcs = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        if hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest() == h:
            if "user" in parsed:
                parsed["user"] = json.loads(parsed["user"])
            return parsed
        return None
    except:
        return None


async def check_subscription(user_id: int) -> dict:
    if not REQUIRED_CHANNELS:
        return {"subscribed": True, "missing": []}
    missing = []
    for ch in REQUIRED_CHANNELS:
        ch = ch.strip()
        if not ch: continue
        try:
            cid = int(ch) if ch.lstrip('-').isdigit() else ch
            m = await bot.get_chat_member(chat_id=cid, user_id=user_id)
            st = m.status.value if hasattr(m.status, 'value') else str(m.status).lower()
            if st in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:
            if "user not found" in str(e).lower():
                missing.append(ch)
    return {"subscribed": len(missing) == 0, "missing": list(dict.fromkeys(missing))}


async def load_telegram_gifts():
    global GIFTS
    try:
        available = await bot.get_available_gifts()
        if not available or not available.gifts:
            for hg in HARDCODED_GIFTS:
                GIFTS[hg["id"]] = {**hg, "is_limited": False, "sold_out": True, "sticker_url": None, "sticker_type": None}
            return False
        
        tg_gifts_map = {tg.id: tg for tg in available.gifts}
        used_tg_ids = set()
        
        for hg in HARDCODED_GIFTS:
            gid = hg["id"]
            tg_id = hg.get("telegram_gift_id")
            
            if tg_id and tg_id in tg_gifts_map:
                tg = tg_gifts_map[tg_id]
                used_tg_ids.add(tg_id)
                is_limited = tg.total_count is not None and tg.total_count > 0
                sold_out = is_limited and (tg.remaining_count or 0) == 0
                sticker_url, sticker_type = await get_sticker_data(tg.sticker)
                GIFTS[gid] = {"id": gid, "title": hg["title"], "emoji": hg["emoji"], "price": tg.star_count, "telegram_gift_id": tg_id, "is_limited": is_limited, "sold_out": sold_out, "total_count": tg.total_count, "remaining_count": tg.remaining_count, "sticker_url": sticker_url, "sticker_type": sticker_type}
            else:
                GIFTS[gid] = {"id": gid, "title": hg["title"], "emoji": hg["emoji"], "price": hg["price"], "telegram_gift_id": None, "is_limited": False, "sold_out": True, "sticker_url": None, "sticker_type": None}
        
        for tg in available.gifts:
            if tg.id in used_tg_ids: continue
            gid = f"tg_{tg.id}"
            is_limited = tg.total_count is not None and tg.total_count > 0
            sold_out = is_limited and (tg.remaining_count or 0) == 0
            sticker_url, sticker_type = await get_sticker_data(tg.sticker)
            GIFTS[gid] = {"id": gid, "title": f"Подарок {tg.star_count}⭐", "emoji": "🎁", "price": tg.star_count, "telegram_gift_id": tg.id, "is_limited": is_limited, "is_unknown": True, "sold_out": sold_out, "total_count": tg.total_count, "remaining_count": tg.remaining_count, "sticker_url": sticker_url, "sticker_type": sticker_type}
        
        print(f"✅ Загружено: {len(GIFTS)} подарков")
        return True
    except Exception as e:
        print(f"❌ Ошибка загрузки подарков: {e}")
        return False


async def get_sticker_data(sticker) -> tuple[str | None, str | None]:
    try:
        file = await bot.get_file(sticker.file_id)
        url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
        if file.file_path:
            if file.file_path.endswith('.webp'): return url, 'webp'
            if file.file_path.endswith('.tgs'): return url, 'tgs'
            if file.file_path.endswith('.webm'): return url, 'webm'
        return url, 'unknown'
    except:
        return None, None


async def send_real_gift(user_id: int, gift_id: str, text: Optional[str] = None) -> tuple[bool, str]:
    gift = GIFTS.get(gift_id)
    if not gift: return False, "Подарок не найден"
    tg_id = gift.get("telegram_gift_id")
    if not tg_id: return False, "Подарок недоступен"
    if gift.get("sold_out"): return False, "Подарок закончился"
    
    try:
        # Без подписи если text не передан или пустой
        if text:
            await bot.send_gift(user_id=user_id, gift_id=tg_id, text=text)
        else:
            await bot.send_gift(user_id=user_id, gift_id=tg_id)
        return True, "OK"
    except Exception as e:
        msg = str(e)
        if "DISALLOWED" in msg.upper(): return False, "🔒 Включи получение подарков"
        if "GIFT_SOLD_OUT" in msg.upper():
            if gift_id in GIFTS: GIFTS[gift_id]["sold_out"] = True
            return False, "😔 Подарок закончился"
        return False, msg[:80]


def roll_case(case_id: str, user_id: int = None) -> dict:
    case = CASES.get(case_id)
    if not case: return {"type": "nothing", "items": [], "multiplier": 1}
    
    if case.get("type") == "stars":
        drops = case.get("drops", [])
        roll = random.random()
        cum = 0
        for d in drops:
            cum += d["chance"]
            if roll < cum:
                return {"type": "stars", "stars_won": d["stars"], "all_drops": drops}
        return {"type": "stars", "stars_won": drops[0]["stars"] if drops else 50, "all_drops": drops}
    
    if case.get("pity_enabled") and user_id:
        new_spent = add_pity_spent(user_id, case["price"])
        if new_spent >= PITY_THRESHOLD:
            reset_pity(user_id)
            return {"type": "gift", "items": [PITY_REWARD_GIFT], "multiplier": 1, "pity_triggered": True, "pity_progress": 0}
        return {"type": "nothing", "items": [], "multiplier": 1, "pity_progress": new_spent}
    
    win_chance = case.get("win_chance", 0.25)
    available = [gid for gid, g in GIFTS.items() if g.get("telegram_gift_id") and not g.get("sold_out") and g["price"] <= case["price"]]
    if not available: return {"type": "nothing", "items": [], "multiplier": 1}
    
    if random.random() < win_chance:
        return {"type": "gift", "items": [random.choice(available)], "multiplier": 1}
    return {"type": "nothing", "items": [], "multiplier": 1}


@router.message(Command("start"))
async def cmd_start(message: Message):
    uid = message.from_user.id
    if is_maintenance_enabled() and not is_admin(uid):
        await message.answer(get_maintenance_text())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🎁 Открыть", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("👋 <b>Подарочница</b>\n\n🎁 Подарки • 🎰 Кейсы • ⭐ Stars", reply_markup=kb, parse_mode=ParseMode.HTML)


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    p = message.successful_payment
    payload = json.loads(p.invoice_payload)
    uid = message.from_user.id
    uname = message.from_user.username
    total = p.total_amount
    itype = payload.get("type")
    pid = payload.get("payment_id")
    
    try:
        if itype == "donate":
            save_donation(uid, uname, total)
            await message.answer(f"💝 Спасибо за донат {total}⭐!")
            return
        
        if itype == "gift":
            gift = GIFTS.get(payload["id"])
            if not gift:
                await message.answer("⚠️ Подарок не найден")
                return
            # Без подписи для обычных покупок
            ok, err = await send_real_gift(uid, payload["id"])
            save_purchase(uid, {"type": "gift", "gift_id": payload["id"], "paid": total})
            await message.answer(f"🎉 {gift['emoji']} {gift['title']}!" if ok else f"⚠️ {err}")
            return
        
        if itype == "case":
            case = CASES.get(payload["id"])
            if not case:
                await message.answer("⚠️ Кейс не найден")
                return
            
            result = roll_case(payload["id"], uid)
            if pid: save_pending_result(pid, result)
            
            if result["type"] == "stars":
                add_star_balance(uid, result["stars_won"])
                await message.answer(f"⭐ +{result['stars_won']} Stars!")
            elif result["type"] == "nothing":
                pity = result.get("pity_progress", 0)
                msg = "😔 Ничего..."
                if pity: msg += f"\n📊 До гарантии: {PITY_THRESHOLD - pity}⭐"
                await message.answer(msg)
            else:
                for gid in result["items"]:
                    await send_real_gift(uid, gid, f"🎰 Из кейса {case['title']}")
                    await asyncio.sleep(0.3)
                g = GIFTS.get(result["items"][0], {})
                msg = f"🎉 {g.get('emoji', '🎁')} {g.get('title', 'Подарок')}!"
                if result.get("pity_triggered"): msg = f"🎯 ГАРАНТ! {msg}"
                await message.answer(msg)
            
            save_purchase(uid, {"type": "case", "case_id": payload["id"], "paid": total})
    except Exception as e:
        print(f"❌ Payment error: {e}")
        await message.answer("⚠️ Произошла ошибка")


async def keep_alive():
    if not SELF_URL: return
    await asyncio.sleep(30)
    async with httpx.AsyncClient() as c:
        while True:
            try: await c.get(f"{SELF_URL}/health", timeout=10)
            except: pass
            await asyncio.sleep(240)


async def refresh_gifts_loop():
    await asyncio.sleep(60)
    while True:
        try: await load_telegram_gifts()
        except: pass
        await asyncio.sleep(600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_google_sheets()
    await load_telegram_gifts()
    await asyncio.sleep(1)
    asyncio.create_task(dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()))
    asyncio.create_task(keep_alive())
    asyncio.create_task(refresh_gifts_loop())
    print("✅ Готово!")
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


class InitDataReq(BaseModel):
    initData: str

class InvoiceReq(BaseModel):
    initData: str
    giftId: str | None = None
    caseId: str | None = None

class PromocodeReq(BaseModel):
    initData: str
    code: str

class CreatePromocodeReq(BaseModel):
    initData: str
    code: str
    rewardType: str
    rewardId: str
    maxUses: int
    signatureText: str = ""

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

class GetResultReq(BaseModel):
    initData: str
    paymentId: str

class SetSignatureReq(BaseModel):
    initData: str
    signature: str


def raise_if_maintenance(uid: int):
    if is_maintenance_enabled() and not is_admin(uid):
        raise HTTPException(503, get_maintenance_text())


async def require_subscription(uid: int):
    sub = await check_subscription(uid)
    if not sub["subscribed"]:
        raise HTTPException(403, "Подпишитесь на каналы")


@app.post("/api/check-subscription")
async def api_check_subscription(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    
    if is_maintenance_enabled() and not is_admin(uid):
        return {"subscribed": False, "maintenance": True, "message": get_maintenance_text(), "channels": []}
    
    result = await check_subscription(uid)
    channels = []
    for ch in REQUIRED_CHANNELS:
        link = CHANNEL_LINKS.get(ch)
        title = ch
        try:
            cid = int(ch) if ch.lstrip('-').isdigit() else ch
            chat = await bot.get_chat(cid)
            title = chat.title or ch
            if not link:
                if chat.username: link = f"https://t.me/{chat.username}"
                elif chat.invite_link: link = chat.invite_link
        except: pass
        channels.append({"id": ch, "title": title, "link": link, "missing": ch in result["missing"]})
    
    return {"subscribed": result["subscribed"], "maintenance": False, "channels": channels}


@app.post("/api/get-user-data")
async def api_get_user_data(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    return {"starBalance": get_star_balance(uid), "pitySpent": get_pity_spent(uid), "pityThreshold": PITY_THRESHOLD, "isAdmin": is_admin(uid), "defaultSignature": get_default_signature()}


@app.get("/api/get-gifts")
async def api_get_gifts():
    gifts = []
    for gid, g in GIFTS.items():
        gifts.append({"id": gid, "title": g.get("title"), "emoji": g.get("emoji", "🎁"), "price": g["price"], "sticker_url": g.get("sticker_url"), "sticker_type": g.get("sticker_type"), "is_limited": g.get("is_limited", False), "is_unknown": g.get("is_unknown", False), "remaining": g.get("remaining_count"), "total": g.get("total_count"), "sold_out": g.get("sold_out", False), "available": g.get("telegram_gift_id") is not None and not g.get("sold_out", False)})
    gifts.sort(key=lambda x: (not x["available"], x["sold_out"], x["price"], x["id"]))
    return {"gifts": gifts, "sales": []}


@app.get("/api/get-cases")
async def api_get_cases():
    cats = {"cheap": {"title": "💰 Дешёвые", "cases": []}, "gifts": {"title": "🎁 Подарки", "cases": []}, "stars": {"title": "⭐ Stars", "cases": []}}
    for cid, c in CASES.items():
        cat = c.get("category", "gifts")
        if cat in cats:
            cats[cat]["cases"].append({"id": cid, "title": c["title"], "price": c["price"], "category": cat, "type": c.get("type"), "pity_enabled": c.get("pity_enabled", False)})
    return {"categories": cats, "pityThreshold": PITY_THRESHOLD}


@app.post("/api/create-invoice")
async def create_invoice(req: InvoiceReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    pid = str(uuid.uuid4())
    
    if req.giftId and req.giftId in GIFTS:
        g = GIFTS[req.giftId]
        if g.get("sold_out") or not g.get("telegram_gift_id"):
            raise HTTPException(400, "Подарок недоступен")
        link = await bot.create_invoice_link(title=f"{g['emoji']} {g['title']}", description=f"Подарок {g['title']}", payload=json.dumps({"type": "gift", "id": req.giftId, "payment_id": pid}), currency="XTR", prices=[LabeledPrice(label=g["title"], amount=g["price"])])
        return {"link": link, "paymentId": pid}
    
    if req.caseId and req.caseId in CASES:
        c = CASES[req.caseId]
        link = await bot.create_invoice_link(title=c["title"], description="🎰 Испытай удачу!", payload=json.dumps({"type": "case", "id": req.caseId, "payment_id": pid}), currency="XTR", prices=[LabeledPrice(label=c["title"], amount=c["price"])])
        return {"link": link, "paymentId": pid}
    
    raise HTTPException(400, "Товар не найден")


@app.post("/api/get-case-result")
async def api_get_case_result(req: GetResultReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    result = get_pending_result(req.paymentId)
    if not result: raise HTTPException(404, "Результат не найден")
    uid = auth["user"]["id"]
    result["newBalance"] = get_star_balance(uid)
    result["pitySpent"] = get_pity_spent(uid)
    if result.get("items"):
        result["won"] = []
        for gid in result["items"]:
            g = GIFTS.get(gid, {})
            result["won"].append({"id": gid, "title": g.get("title"), "emoji": g.get("emoji", "🎁"), "price": g.get("price", 0), "sticker_url": g.get("sticker_url"), "sticker_type": g.get("sticker_type")})
    return result


@app.post("/api/buy-with-balance")
async def api_buy_with_balance(req: BuyWithBalanceReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    uid = auth["user"]["id"]
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    bal = get_star_balance(uid)
    
    if req.giftId and req.giftId in GIFTS:
        g = GIFTS[req.giftId]
        if g.get("sold_out") or not g.get("telegram_gift_id"):
            raise HTTPException(400, "Подарок недоступен")
        price = g["price"]
        if bal < price:
            raise HTTPException(400, f"Нужно {price}⭐, у вас {bal}⭐")
        use_star_balance(uid, price)
        ok, err = await send_real_gift(uid, req.giftId)
        save_purchase(uid, {"type": "gift_balance", "gift_id": req.giftId, "paid": price})
        if not ok:
            add_star_balance(uid, price)
            raise HTTPException(500, err)
        return {"success": True, "type": "gift", "reward": f"{g['emoji']} {g['title']}", "newBalance": get_star_balance(uid)}
    
    if req.caseId and req.caseId in CASES:
        c = CASES[req.caseId]
        price = c["price"]
        if bal < price:
            raise HTTPException(400, f"Нужно {price}⭐, у вас {bal}⭐")
        use_star_balance(uid, price)
        result = roll_case(req.caseId, uid)
        
        if result["type"] == "stars":
            add_star_balance(uid, result["stars_won"])
            return {"success": True, "type": "stars", "starsWon": result["stars_won"], "newBalance": get_star_balance(uid), "allDrops": result.get("all_drops", [])}
        
        if result["type"] == "nothing":
            return {"success": True, "type": "nothing", "newBalance": get_star_balance(uid), "pitySpent": result.get("pity_progress", 0)}
        
        sent = []
        for gid in result["items"]:
            g = GIFTS.get(gid, {})
            ok, _ = await send_real_gift(uid, gid, f"🎰 Из кейса {c['title']}")
            if ok:
                sent.append({"id": gid, "title": g.get("title"), "emoji": g.get("emoji", "🎁"), "price": g.get("price", 0), "sticker_url": g.get("sticker_url"), "sticker_type": g.get("sticker_type")})
            await asyncio.sleep(0.3)
        
        return {"success": True, "type": "gifts", "won": sent, "multiplier": result.get("multiplier", 1), "newBalance": get_star_balance(uid), "pityTriggered": result.get("pity_triggered", False), "pitySpent": get_pity_spent(uid)}
    
    raise HTTPException(400, "Не указан товар")


@app.post("/api/create-donate")
async def create_donate(req: DonateReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    if req.amount < 1 or req.amount > 10000:
        raise HTTPException(400, "Сумма от 1 до 10000")
    link = await bot.create_invoice_link(title="💝 Донат", description=f"Поддержка проекта на {req.amount}⭐", payload=json.dumps({"type": "donate", "amount": req.amount}), currency="XTR", prices=[LabeledPrice(label="Донат", amount=req.amount)])
    return {"link": link}


@app.post("/api/activate-promocode")
async def api_activate_promocode(req: PromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth: raise HTTPException(401, "Invalid auth")
    uid = int(auth["user"]["id"])
    raise_if_maintenance(uid)
    await require_subscription(uid)
    
    code = req.code.strip().upper()
    promos = get_promocodes()
    
    if code not in promos:
        raise HTTPException(404, "Промокод не найден")
    
    p = promos[code]
    if uid in p.get("used_by", []):
        raise HTTPException(400, "Вы уже использовали этот промокод")
    if p.get("uses", 0) >= p.get("max_uses", 0):
        raise HTTPException(400, "Промокод закончился")
    
    p["uses"] = p.get("uses", 0) + 1
    p.setdefault("used_by", []).append(uid)
    save_promocode(code, p)
    
    rt, ri = p["reward_type"], p["reward_id"]
    
    if rt == "stars":
        amt = int(ri)
        add_star_balance(uid, amt)
        return {"success": True, "reward": f"+{amt}⭐", "newBalance": get_star_balance(uid)}
    
    if rt == "gift":
        g = GIFTS.get(ri)
        if g and g.get("telegram_gift_id"):
            # Используем кастомную подпись из промокода
            sig_text = p.get("signature_text", "")
            ok, err = await send_real_gift(uid, ri, sig_text if sig_text else None)
            if ok:
                return {"success": True, "reward": f"{g['emoji']} {g['title']}"}
            return {"success": False, "error": err}
    
    if rt == "case":
        result = roll_case(ri, uid)
        return {"success": True, "reward": "Кейс открыт!", "caseResult": result}
    
    return {"success": True, "reward": "OK"}


@app.post("/api/admin/get-promocodes")
async def api_admin_get_promocodes(req: InitDataReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    promos = get_promocodes()
    return {"promocodes": [{"code": c, "rewardType": p["reward_type"], "rewardId": p["reward_id"], "maxUses": p["max_uses"], "uses": p["uses"], "created": p.get("created", ""), "signatureText": p.get("signature_text", "")} for c, p in promos.items()]}


@app.post("/api/admin/create-promocode")
async def api_admin_create_promocode(req: CreatePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    code = req.code.strip().upper()
    if not code:
        raise HTTPException(400, "Код не указан")
    save_promocode(code, {"reward_type": req.rewardType, "reward_id": req.rewardId, "max_uses": req.maxUses, "uses": 0, "used_by": [], "created": datetime.now().isoformat(), "signature_text": req.signatureText})
    return {"success": True, "code": code}


@app.post("/api/admin/delete-promocode")
async def api_admin_delete_promocode(req: DeletePromocodeReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    delete_promocode(req.code.upper())
    return {"success": True}


@app.post("/api/admin/set-default-signature")
async def api_admin_set_default_signature(req: SetSignatureReq):
    auth = validate_init_data(req.initData)
    if not auth or not is_admin(auth["user"]["id"]):
        raise HTTPException(403, "Admin only")
    set_setting("default_signature", req.signature)
    return {"success": True}


@app.get("/api/admin/get-gifts-list")
async def api_admin_get_gifts_list():
    gifts_list = [{"id": gid, "title": f"{g['emoji']} {g['title']} ({g['price']}⭐)", "price": g["price"]} for gid, g in sorted(GIFTS.items(), key=lambda x: (x[1]["price"], x[0])) if g.get("telegram_gift_id")]
    cases_list = [{"id": cid, "title": c["title"], "price": c["price"]} for cid, c in CASES.items()]
    return {"gifts": gifts_list, "cases": cases_list}


@app.get("/api/refresh-gifts")
async def refresh_gifts():
    ok = await load_telegram_gifts()
    return {"success": ok, "gifts_count": len(GIFTS), "available": sum(1 for g in GIFTS.values() if g.get("telegram_gift_id") and not g.get("sold_out"))}


@app.get("/")
async def root():
    return {"app": "Подарочница v12.0", "gifts": len(GIFTS)}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))

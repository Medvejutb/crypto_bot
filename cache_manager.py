import redis.asyncio as redis
import asyncio, json, time
from utils.views import Logging_manager

logger = Logging_manager.get_logger()

def cache_guard(func):
    """Декоратор, который ловит любые ошибки Redis
    и ссыт в лог"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[CACHE ERROR] функция {func.__name__} ёбнулась: {e!r}")
            return None
    return wrapper


class Cache_manager:
    def __init__(self):
        self.r = redis.Redis(
            host="localhost", port=6379, db=0,
            decode_responses=True, encoding="utf-8"
        )
    
    @cache_guard
    async def clear(self):
        await self.r.flushall()

    @cache_guard
    async def get_all_symbols(self):
        cursor = 0
        symbols = set()

        while True:
            cursor, keys = await self.r.scan(cursor=cursor, match="*:*:*", count=1000)
            for key in keys:
                parts = key.split(":")
                if len(parts) == 3:
                    symbols.add(parts[0])
            if cursor == 0:
                break

        return sorted(list(symbols))
        


    # ---------------- PRICES ---------------- #

    @cache_guard
    async def set_price(self, symbol: str, exchange: str,
                        price: float, life: int = 5):
        key = f"{symbol}:{exchange}:PRICE"
        payload = {"value": price, "ts": time.time()}
        await self.r.set(key, json.dumps(payload), ex=life)

    @cache_guard
    async def get_price(self, symbol: str, exchange: str):
        key = f"{symbol}:{exchange}:PRICE"
        raw = await self.r.get(key)
        if not raw:
            return None
        return json.loads(raw)["value"]

    @cache_guard
    async def is_price_old(self, symbol: str, exchange: str,
                           max_age: int = 5):
        key = f"{symbol}:{exchange}:PRICE"
        raw = await self.r.get(key)
        if not raw:
            return True
        ts = json.loads(raw)["ts"]
        return (time.time() - ts) > max_age

    # --------------- FUNDING ---------------- #

    @cache_guard
    async def set_funding(self, symbol: str, exchange: str,
                          funding: float, next_time: float | None):
        key = f"{symbol}:{exchange}:FUNDING"
        payload = {"value": funding, "next_time": next_time}
        await self.r.set(key, json.dumps(payload), ex=60*5)

    @cache_guard
    async def set_unsupported_funding(self, symbol: str, exchange: str):
        key = f"{symbol}:{exchange}:FUNDING"
        payload = {"value": 'unsupported'}
        await self.r.set(key, json.dumps(payload), ex=9 * 3600)


    @cache_guard
    async def get_funding(self, symbol: str, exchange: str):
        key = f"{symbol}:{exchange}:FUNDING"
        raw = await self.r.get(key)
        return json.loads(raw)["value"] if raw else None
    

    @cache_guard
    async def get_next_funding_time(self, symbol: str, exchange: str):
        key = f"{symbol}:{exchange}:FUNDING"
        raw = await self.r.get(key)
        if not raw:
            return None

        try:
            payload = json.loads(raw)
            next_time = payload.get("next_time")
            return float(next_time) if isinstance(next_time, (int, float, str)) and str(next_time).replace('.', '', 1).isdigit() else None
        except Exception:
            return None

    @cache_guard
    async def is_funding_old(self, symbol: str, exchange: str):
        next_time = await self.get_next_funding_time(symbol, exchange)
        # Если next_time не число — сразу до свидания
        if not isinstance(next_time, (int, float)):
            return True
        return time.time() >= next_time
    
    # -------------PRICE_FUNDING_IN_DICT----------#

    @cache_guard
    async def set_symbol_data(self,
                                     symbol: str,
                                     exchange: str,
                                     price: float,
                                     funding: float,
                                     next_time: float | None
                                     ):
        key = f'{symbol}:{exchange}:PRICE_FUNDING_DICT'
        value = {
            'price': price,
            'funding': funding,
            'next_time': next_time,
        }
        await self.r.set(key, json.dumps(value), ex=30)
    
    async def get_symbol_data(self,
                                     symbol: str,
                                     exchange: str,
                                     ):
        key = f'{symbol}:{exchange}:PRICE_FUNDING_DICT'
        raw = await self.r.get(key)
        return json.loads(raw) if raw else None

    # ------------- UNCORRELATIONS ------------ #

    @cache_guard
    async def set_uncor(self, uncor_dict: dict):
        await self.r.set("LAST_UNCORRELATIONS", json.dumps(uncor_dict), ex=1)

    @cache_guard
    async def get_uncor(self):
        raw = await self.r.get("LAST_UNCORRELATIONS")
        return json.loads(raw) if raw else None
    
    # -------------ACTIVE_SYMBOL_PAIRS_IN_POSITIONS-------------#

    @cache_guard
    async def add_active_position_symbol_pair(self, key):
        raw = await self.r.get("ACTIVE_POS_PAIR")
        combos = json.loads(raw) if raw else []

        new_combo = list(key)

        if new_combo not in combos:
            combos.append(new_combo)
            await self.r.set("ACTIVE_POS_PAIR", json.dumps(combos))


    @cache_guard
    async def get_active_position_symbol_pairs(self) -> list[set] | None:
        raw = await self.r.get("ACTIVE_POS_PAIR")
        if not raw:
            return None

        combos = json.loads(raw)
        return [tuple(c) for c in combos]

    @cache_guard
    async def del_active_position_symbol_pair(self, key: tuple):
        raw = await self.r.get("ACTIVE_POS_PAIR")
        if not raw:
            return

        combos = json.loads(raw)
        target = list(key)

        if target in combos:
            combos.remove(target)
            await self.r.set("ACTIVE_POS_PAIR", json.dumps(combos))
    
    #----------------POSITIONS_MESSAGES_FOR_ALERT--------------#
    # @cache_guard
    # async def set_last_positions_messages(self, order_data):
    #     key = 'POSITION_MESSAGES'

    #     raw = await self.get_last_positions_messages()

    #     if not raw:
    #         return None
        
    #     for pair in raw:
    #         if order_data['pair_key'] == raw['pair_key']:
    #             raw[pair]

    #     value_= order_data

    #     await self.r.set(key, json.dumps(value), ex=30)

    # @cache_guard
    # async def get_last_positions_messages(self) -> list:
    #     raw = await self.r.get("POSITION_MESSAGES")
    #     if not raw:
    #         return None

    #     combos = json.loads(raw)
    #     return [c for c in combos]
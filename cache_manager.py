import redis
import json
import time
from utils.views import Logging_manager

logger = Logging_manager.get_logger()

class Cache_manager:

    def __init__(self):

        self.r = redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
        )
    
    def set_symbol_data(self, symbol, stock, funding=None, next_time=None, last_time=None):
        existing = self.get_symbol_funding_data(symbol, stock)
        if existing is None:
            existing = {
            'funding': funding,
            'next_time': next_time,
            'last_time': last_time
        }
        else:
            if funding is not None:
                existing["funding"] = funding
            if next_time is not None:
                existing["next_time"] = next_time
            if last_time is not None:
                existing["last_time"] = last_time

        key = f"funding:{symbol}:{stock}"
        self.r.set(key, json.dumps(existing))
        logger.debug(f'[CACHE SYSTEM] Символ {symbol} биржи {stock} ДОБАВЛЕН / ОБНОВЛЁН в кэше')

    def add_symbol_funding_para(self, symbol, stock, funding=None, next_time=None, last_time=None):
        key = f"funding:{symbol}:{stock}"
        value = {
            'funding': funding,
            'next_time': next_time,
            'last_time': last_time
        }
        self.r.set(key, json.dumps(value))

        
    def get_symbol_funding_data(self, symbol, stock):
        key = f"funding:{symbol}:{stock}"
        data = self.r.get(key)
        if data:
            return json.loads(data)
        return None
    
    def get_symbol_funding_next_time(self, symbol, stock):
        next_funding_time = self.get_symbol_funding_data(symbol, stock)['next_time']
        if next_funding_time is None:
            return None
        return int(next_funding_time)


    def update_symbol(self, symbol, stock, funding=None, next_time=None, last_time=None):
        existing = self.get_symbol_funding_data(symbol, stock)
        if not existing:
            return False

        if funding is not None:
            existing["funding"] = funding
        if next_time is not None:
            existing["next_time"] = next_time
        if last_time is not None:
            existing["last_time"] = last_time

        key = f"funding:{symbol}:{stock}"
        self.r.set(key, json.dumps(existing))
        logger.debug(f'[CACHE SYSTEM] Символ {symbol} биржи {stock} ОБНОВЛЕЁН в кэше')

        return True


    def get_symbols_list(self):
        keys = self.r.keys("funding:*")
        return [k.split("funding:")[1].split(":") for k in keys]  # вернёт [symbol, stock]

    
    def is_funding_expired(self, symbol, stock):

        symbol_data = self.get_symbol_funding_data(symbol, stock)

        if symbol_data is None or symbol_data['next_time'] - time.time() <= 0:
            return True
        
        return False


    def check_none_funding(self) -> list:
        result = []
        for symbol, stock in self.get_symbols_list():
            data = self.get_symbol_funding_data(symbol, stock)
            if data and data['funding'] is None:
                result.append((symbol, stock))
        return result



    def del_symbol(self, symbol, stock):
        self.r.delete(f'funding:{symbol}:{stock}')
        logger.debug(f'[SYSTEM] Символ {symbol} биржи {stock} УДАЛЁН из кэша')
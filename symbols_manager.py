import asyncio
import models

from utils.views import Logging_manager

logger = Logging_manager.get_logger()

def worker_guard(func):
    """Декоратор, который ловит любые ошибки
    и ссыт в лог"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[ERROR] функция {func.__name__} ёбнулась: {e!r}")
            return None
    return wrapper

"""
                        self.data[data['s']] = {
                            'price': float(data['c']),
                            'funding': None,
                            'next_funding_time': None,
                            'time': int(data['E'])
                        }
"""

{
    'event': 'price',
    'symbol': 'PIZDAUSDT',
    'exchange': 'PIZDA',
    'price': '228',
}
{
    'event': 'funding',
    'symbol': 'PIZDAUSDT',
    'exchange': 'PIZDA',
    'funding': '228',
    'next_time': '228'
}
{
    'event': 'uncor',
    'symbol': 'PIZDAUSDT',
    'higher': 'PIZDA',
    'lower': 'PIZDA',
    'uncorrelation': '228'
}
{
    'event': 'position',
    'symbol': 'PIZDAUSDT',
    'higher': 'PIZDA',
    'lower': 'PIZDA',
    'price': '228',
}

class Symbols_manager:
    """
    TODO: Возможно эта дрисня не понадобится
    """
    def __init__(self):
        self._symbols: dict[str, models.Symbol]
        self._symbols_queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._handlers = {
            'market.price': self._update_price,
            'market.funding': self._update_funding,
            'uncorrelation': self._update_uncorrelation,
            'position': self._update_position,
        }

    async def run(self):
        while True:
            raw = await self._symbols_queue.get()
            event_type = raw.get('type')
            symbol = raw.get('symbol')
            exchange = raw.get('exchange')

            if symbol not in self._symbols:
                self._create_symbol_obj(symbol=symbol)
                if exchange not in self._symbols[symbol].exchanges:
                    self._create_exch_obj(symbol=symbol, exchange=exchange)

            handler = self._handlers[event_type]
            handler(raw)

    def _update_price(self, symbol, exch, price):
        self._symbols[symbol].exchanges[exch].price = price

    def _update_funding(self, symbol, exch, funding, next_time):
        self._symbols[symbol].exchanges[exch].funding = funding
        self._symbols[symbol].exchanges[exch].next_funding = next_time
    
    def _update_uncorrelation(self):
        pass
    
    def _update_position(self):
        pass

    def _create_exch_obj(
            self,
            symbol: str,
            exchange: str,
        ):
        self._symbols[symbol].exchanges[exchange] = models.SymbolExchange(
            symbol=symbol,
            exchange=exchange,
        )
        
    def _create_symbol_obj(
            self,
            symbol: str,
            ):
        self._symbols[symbol] = models.Symbol(
            symbol=symbol,
            exchanges={},
            spreads={},
            uncorrelations={},
            positions={},
        )

    # def _update_exch_data(
    #         self,
    #         symbol,
    #         exchange,
    #         price=None,
    #         funding=None,
    #         next_funding=None,
    # ):
    #     """
    #     Универсальное обновление символа
    #     """
    #     symbol_exch_obj = self._symbols[symbol].exchanges[exchange]

    #     if price:
    #         symbol_exch_obj.price = price
    #     if funding:
    #         symbol_exch_obj.funding = funding
    #     if next_funding:
    #         symbol_exch_obj.next_funding = next_funding


    async def update(self, data):
        await self._symbols_queue.put(data)

    def get_symbol(self):
        pass


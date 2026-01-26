import msgspec
import asyncio
from abc import ABC, abstractmethod
#import logger cacche
from decimal import Decimal
from typing import Literal, List
from utils.views import Logging_manager
from cache_tools import Cache
from exchanges.exch_register import EXCHANGE_REGISTRY

logger = Logging_manager.get_logger()

class Base_exchange(ABC):

    def __init_subclass__(cls):
        if not hasattr(cls, "name"):
            raise RuntimeError(
                f"{cls.__name__} не задал name, иди нахуй"
            )
        EXCHANGE_REGISTRY[cls.name] = cls

    def __init__(self, to_manager_queue):
        super().__init__()
        self.exchange_name: str = self.__class__.name
        self.symbols: dict[str, SymbolExchange] = {}
        self.ready_event = asyncio.Event()
        self.connection = False
        self.session = None
        self.to_manager_queue: asyncio.Queue[List[SymbolExchange]] = to_manager_queue
        self.API_KEY: str = ''
        self.SECTRET_KEY: str = ''
        self.cache = Cache
        self._is_ready = False
        self.base_url = ''
        self.socket_url = ''

    @abstractmethod 
    async def _ping_loop(self):
        pass

    async def _wait_ready(self):
        while len(self.symbols) < 30:
            await asyncio.sleep(0.5)
        self.logger.success(f'[{self.exchange_name}][SOCKET]\nДанных достаточно. Биржа готова.')
        self._is_ready = True
        self.ready_event.set()

    @abstractmethod
    async def _get_symbols(self):
        """
        Выгрузка по API всех торгующихся фьючей
        """
        pass

    @abstractmethod
    async def run(self):
        pass

    def connect(self):
        if self.connection:
            return True
        return False
    
    @abstractmethod
    async def get_funding(self, symbol_list: List[str]) -> Decimal:
        pass

    @abstractmethod
    def _convert_usd_contracts(
        self,
        usd_amount: Decimal,
        price: Decimal,
        symbol: str,
    ):
        pass

    @abstractmethod
    def _make_signature(
        self,
        query_str: str,
        secret_key: str,
    ):
        pass

    @abstractmethod
    async def place_order(
        self,
        symbol: str,
        side: str,
        volume: Decimal,
        price: Decimal,
        **kwargs
    ):
        """
        Размещает маркет-ордер на Binance Futures.
        symbol – инструмент, например 'BTCUSDT'
        side – 'BUY' или 'SELL'
        usd_count – сумма в USDT
        price – текущая цена (для конвертации в контракты)
        """
        pass

class PairMetrick(msgspec.Struct):
    value: Decimal

class Position(msgspec.Struct):
    symbol: str
    higher_exchange: str
    lower_exchange: str
    start_time: int
    uncorrelation: Decimal
    higher_price: Decimal
    lower_price: Decimal
    state: Literal['wait', 'active', 'closing', 'closed', 'cancel']
    
class Order_rules(msgspec.Struct):
    symbol: str
    exchange: str

class SymbolExchange(msgspec.Struct):
    symbol: str
    exchange: str
    price: Decimal | None = None
    funding: Decimal | None = None
    next_funding: int | None = None
    last_funding_update: int = None
    order_rules: Order_rules | None = None


class Symbol(msgspec.Struct):
    symbol: str
    exchanges: dict[str, SymbolExchange] = msgspec.field(default_factory=dict) # тут будут храниться SymbolExchange по бирже
    spreads: dict[tuple[str, str], "PairMetrick"] = msgspec.field(default_factory=dict) # тут ключи вида (символ, биржа, биржа)
    uncorrelations: dict[tuple[str, str], "PairMetrick"] = msgspec.field(default_factory=dict) # тут ключи вида (символ, биржа, биржа)
    positions: dict[tuple[str, str], "Position"] = msgspec.field(default_factory=dict) # тут ключи вида (символ, биржа, биржа)

class Context(msgspec.Struct):
    sockets_snapshot: dict[str, Symbol] = None
    best_spreads: dict[str, dict[tuple[str, str], "PairMetrick"]] = None
    uncorrelations: dict[str, dict[tuple[str, str], "PairMetrick"]] = None
    active_pos_symbols: dict[tuple[str, str, str], Position] = None

    def clear(self):
        self.sockets_snapshot = {}
        self.best_spreads = {}
        self.uncorrelations = {}
        self.active_pos_symbols = {}
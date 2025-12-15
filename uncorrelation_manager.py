import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
import json
import time
from itertools import combinations
from pprint import pprint

from utils.math_operations import Calculator

@dataclass(slots=True)
class Symbol:
    """
    TODO: 
    - Есть варик добавить поле с биржами, которые уже учавствую в позиции,
    и инструменты для работы с этим говном
    """
    symbol: str
    exchanges: dict = field(default_factory=lambda: {
        'binance': None,
        'bitget': None,
        'okx': None,
        'bybit': None
    })
    spreads: dict[str, Decimal] = field(default_factory=dict)

@dataclass(slots=True)
class Symbol_exchange_data:
    symbol: str
    exchange: str
    price: Decimal = None
    funding: Decimal = None
    next_funding_time: Decimal = None

@dataclass(slots=True)
class Context:
    raw_sockets_data: dict = None
    best_spreads:dict = None
    uncorrelations: dict = None
    active_pos_symbols: list = None

    def clear(self):
        self.raw_sockets_data = {}
        self.best_spreads = {}
        self.uncorrelations = {}
        self.active_pos_symbols = []


class Uncorrelation_manager:
    def __init__(
        self,
        logger,
        cache_manager,
        funding_manager,
        bot,
        chat_id,
        interval,
        exchanges,
        spread,
        get_raw_sockets_data,
        sockets_ready_event,
    ):
        self.logger = logger
        self.cache_manager = cache_manager
        self.funding_manager = funding_manager
        self.uncorrelations_list = []
        self.calculator = Calculator()
        self.bot = bot
        self.chat_id = chat_id
        self.interval = interval
        self.exchanges = exchanges
        self.conf_spread = spread

        self.symbols_data = {}
        self.context = Context()
        self.pipeline_steps = [
            self._process_raw_data,
            self._get_spreads,
            self._get_best_spread,
            self._update_fundings_for_best_spreads,
            self._set_uncorrelation,
        ]

        self.get_raw_func = get_raw_sockets_data
        '''
        Формат высера из sockets_data:
            'AAVEUSDT': {'binance': {'funding': None,
                                    'next_funding_time': None,
                                    'price': 165.72,
                                    'time': 1763919928111},
                        'bitget': {'funding': -0.0072,
                                    'next_funding_time': '1763942400000',
                                    'price': '165.56',
                                    'time': '1763919928467'},
                        'bybit': {'price': 165.57, 'time': 1763919928781},
                        'okx': {'ctVal': '0.1',
                                'funding': None,
                                'instId': 'AAVE-USDT-SWAP',
                                'lotSz': '0.1',
                                'minSz': '0.1',
                                'next_funding_time': None,
                                'price': 165.56,
                                'time': 1763919928833}},
        '''
        self.sockets_is_ready = sockets_ready_event

        self.uncorrelations_event = asyncio.Event()

    async def _process_raw_data(self, context):
        """
        Обработка сырых данных с сокетов.

        Создания объекта нового символа, в котором хранятся еще несколько
        объектов под каждую биржу. Если символ или биржа символа уже существует,
        то будет просто обновление.
                
        Прайсы и фандинги сразу оборачиваются в Децимал, чтобы дальше
        с этим говном можно было проводить математические операции.
        """
        raw = context.raw_sockets_data

        for symbol, exchanges in raw.items():

            if symbol == 'stock':
                continue

            if symbol not in self.symbols_data:
                self.symbols_data[symbol] = Symbol(
                    symbol=symbol,
                )
            
            symbol_obj = self.symbols_data.get(symbol)

            for exchange, data in exchanges.items():

                price = data.get('price')
                funding = data.get('funding')
                nft = data.get('next_funding_time')

                if symbol_obj.exchanges.get(exchange) is None:

                    symbol_obj.exchanges[exchange] = Symbol_exchange_data(
                        symbol=symbol,
                        exchange=exchange,
                    )

                symbol_exch_obj = symbol_obj.exchanges.get(exchange)

                if price is not None:
                    symbol_exch_obj.price = Decimal(str(price))

                if funding is not None:
                    symbol_exch_obj.funding = Decimal(str(funding))

                if nft is not None:
                    symbol_exch_obj.next_funding_time = Decimal(nft)

    async def _get_active_pos_symbol(self):
        """
        Собирает из кэша актуальные пары, которые учавствуют в позициях
        и обновляет self.active_pos_symbols
        """
        active_pos_symbols = await self.cache_manager.get_active_position_symbol_pairs() or []
        return active_pos_symbols

    async def _get_spreads(self, context):
        """
        Получение новых разниц в прайсах в специальный дикт.

        Это говно проходится по всем символам всех бирж.

        Формат хранения разниц:

        key = tuple(sorted([symbol, higher_exchange, lower_exchange]))

        {
            key: int,
        }
        """
        for symbol_obj in self.symbols_data.values():
            exchanges = list(symbol_obj.exchanges.values())
            for exch1, exch2 in combinations(exchanges, 2):

                if exch1 is None or exch2 is None:
                    continue

                price1 = exch1.price
                price2 = exch2.price

                if price1 == 0 or price2 == 0:
                    continue
                if price1 is None or price2 is None:
                    continue

                spread = self.calculator.calc_spread(
                    price_1=price1,
                    price_2=price2,
                )
                key = tuple(sorted((exch1.exchange, exch2.exchange)))
                symbol_obj.spreads[key] = spread       

    async def _get_best_spread(self, context):
        active_pos_symbols = await self._get_active_pos_symbol()
        context.active_pos_symbols = active_pos_symbols
        # if active_pos_symbols: pprint(active_pos_symbols)

        for symbol_obj in self.symbols_data.values():
            best_spread = None
            best_key = None

            for key, spread in sorted(symbol_obj.spreads.items(), key=lambda x: -abs(x[1])):
                exch1, exch2 = key
                global_key = (symbol_obj.symbol, *sorted([exch1, exch2]))

                if global_key in active_pos_symbols:
                    best_spread = spread
                    best_key = key
                    break

                if abs(spread) >= self.conf_spread and best_spread is None:
                    best_spread = spread
                    best_key = key

            if best_key:
                context.best_spreads[(symbol_obj.symbol, *best_key)] = best_spread

    async def _update_fundings_for_best_spreads(self, context):

        key_list = []

        for key, value in context.best_spreads.items():

            symbol, exch1, exch2 = key

            for exch in (exch1, exch2):
                key = (symbol, exch)
                key_list.append(key)
        
        fundings_dict = await self.funding_manager.get_fundings(key_list)


        if fundings_dict:
        
            for key, data in fundings_dict.items():
                symbol, exchange = key
                funding = str(data['funding']) or None
                next_funding_time = str(data['next_funding_time'])

                self.symbols_data[symbol].exchanges[exchange].funding = Decimal(funding)
                self.symbols_data[symbol].exchanges[exchange].next_funding_time = Decimal(next_funding_time)

    async def _set_uncorrelation(self, context):
        """Нахожу раскорреляции"""
        uncorrelation_dict = {}
        for key, value in context.best_spreads.items():
            symbol, exch1, exch2 = key
            symbol_obj = self.symbols_data[symbol]

            exch1_obj = symbol_obj.exchanges[exch1]
            exch2_obj = symbol_obj.exchanges[exch2]

            funding1 = exch1_obj.funding
            funding2 = exch2_obj.funding

            if funding1 is None or funding2 is None:
                continue
            
            if not self.funding_manager.is_fundings_times_same(
                exch1_obj.next_funding_time,
                exch2_obj.next_funding_time
            ):
                continue

            stock1 = {
                'price': exch1_obj.price,
                'funding': exch1_obj.funding,
                'next_funding_time': exch1_obj.next_funding_time,
            }
            stock2 = {
                'price': exch2_obj.price,
                'funding': exch2_obj.funding,
                'next_funding_time': exch2_obj.next_funding_time,
            }
            
            result = self.calculator.calc_uncorrelation(
                stock1,
                stock2,exch1,
                exch2,
                active_pos=True if key in context.active_pos_symbols else False)
            if result:
                uncorrelation_dict[symbol] = result
                self.uncorrelations_event.set()
            
        await self.cache_manager.set_uncor(uncorrelation_dict)

    async def start_work(self):
        await self.sockets_is_ready.wait()

        while True:

            self.context.clear()
            self.context.raw_sockets_data = self.get_raw_func()

            for step in self.pipeline_steps:
                await step(context=self.context)
            await asyncio.sleep(0.5)


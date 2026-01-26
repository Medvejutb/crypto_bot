import asyncio
from decimal import Decimal
from itertools import combinations
from pprint import pprint
from models import Context, Symbol, SymbolExchange
from utils.math_operations import Calculator

# @dataclass(slots=True)
# class Symbol:
#     """
#     TODO: 
#     - Есть варик добавить поле с биржами, которые уже учавствую в позиции,
#     и инструменты для работы с этим говном
#     """
#     symbol: str
#     exchanges: dict = field(default_factory=lambda: {
#         'binance': None,
#         'bitget': None,
#         'okx': None,
#         'bybit': None
#     })
#     spreads: dict[str, Decimal] = field(default_factory=dict)

# @dataclass(slots=True)
# class Symbol_exchange_data:
#     symbol: str
#     exchange: str
#     price: Decimal = None
#     funding: Decimal = None
#     next_funding_time: Decimal = None

# @dataclass(slots=True)
# class Context:
#     raw_sockets_data: dict = None
#     best_spreads:dict = None
#     uncorrelations: dict = None
#     active_pos_symbols: list = None

#     def clear(self):
#         self.raw_sockets_data = {}
#         self.best_spreads = {}
#         self.uncorrelations = {}
#         self.active_pos_symbols = []


class Uncorrelation_manager:
    def __init__(
        self,
        logger,
        cache_manager,
        funding_manager,
        interval,
        exchanges,
        spread,
        from_sockets_queue,
        sockets_ready_event,
    ):
        self.logger = logger
        self.cache_manager = cache_manager
        self.funding_manager = funding_manager
        self.uncorrelations_list = []
        self.calculator = Calculator()
        self.interval = interval
        self.exchanges = exchanges
        self.conf_spread = spread

        self.context = Context()
        self.pipeline_steps = [
            self._get_spreads,
            self._get_best_spread,
            self._set_uncorrelation,
        ]

        self.from_sockets_queue: asyncio.Queue = from_sockets_queue

        self.sockets_is_ready: asyncio.Event = sockets_ready_event

        self.uncorrelations_event = asyncio.Event()

    async def _get_active_pos_symbol(self):
        """
        Собирает из кэша актуальные пары, которые учавствуют в позициях
        и обновляет self.active_pos_symbols
        """
        active_pos_symbols = await self.cache_manager.get_active_position_symbol_pairs() or []
        return active_pos_symbols

    async def _get_spreads(self, context: Context) -> None:
        """
        Получение новых разниц в прайсах в специальный дикт.

        Это говно проходится по всем символам всех бирж.

        Формат хранения разниц:

        key = tuple(sorted([symbol, higher_exchange, lower_exchange]))

        TODO: возможны туплы с повторяющимеся биржами

        """

        for symbol_obj in context.sockets_snapshot.values():
            symbol = symbol_obj.symbol
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

                context.sockets_snapshot[symbol].spreads[key] = spread       

    async def _get_best_spread(self, context: Context) -> None:
        """
        
        """

        active_pos_symbols = await self._get_active_pos_symbol()
        context.active_pos_symbols = active_pos_symbols
        # if active_pos_symbols: pprint(active_pos_symbols)

        for symbol_name, symbol_obj in context.sockets_snapshot.items():
            if not symbol_obj.spreads:
                continue

            pair, value = max(
                symbol_obj.spreads.items(),
                key=lambda item: item[1].spread
            )

            context.best_spreads[symbol_name] = {
                pair: value
            }

    async def _set_uncorrelation(self, context: Context) -> None:
        """Нахожу раскорреляции"""

        for symbol, spread in context.best_spreads.items():
            exch1, exch2 = spread


        # uncorrelation_dict = {}
        # for key, value in context.best_spreads.items():
        #     symbol, exch1, exch2 = key
        #     symbol_obj = self.symbols_data[symbol]

        #     exch1_obj = symbol_obj.exchanges[exch1]
        #     exch2_obj = symbol_obj.exchanges[exch2]

        #     funding1 = exch1_obj.funding
        #     funding2 = exch2_obj.funding

        #     if funding1 is None or funding2 is None:
        #         continue
            
        #     if not self.funding_manager.is_fundings_times_same(
        #         exch1_obj.next_funding_time,
        #         exch2_obj.next_funding_time
        #     ):
        #         continue

        #     stock1 = {
        #         'price': exch1_obj.price,
        #         'funding': exch1_obj.funding,
        #         'next_funding_time': exch1_obj.next_funding_time,
        #     }
        #     stock2 = {
        #         'price': exch2_obj.price,
        #         'funding': exch2_obj.funding,
        #         'next_funding_time': exch2_obj.next_funding_time,
        #     }
            
        #     result = self.calculator.calc_uncorrelation(
        #         stock1,
        #         stock2,exch1,
        #         exch2,
        #         active_pos=True if key in context.active_pos_symbols else False)
        #     if result:
        #         uncorrelation_dict[symbol] = result
        #         self.uncorrelations_event.set()
            
        # await self.cache_manager.set_uncor(uncorrelation_dict)

    async def start_work(self) -> None:
        """
        TODO:
        
        редиска должна вызываться только в начале тика

        self.symbols_data пересоздается каждый тик, чтобы не держать старые данные
        """
        await self.sockets_is_ready.wait()

        while True:

            self.context.clear()
            self.context.sockets_snapshot = await self.from_sockets_queue.get()

            for step in self.pipeline_steps:
                await step(context=self.context)


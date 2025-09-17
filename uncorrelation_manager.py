import asyncio
import json
import time
from itertools import combinations
from pprint import pprint

from utils.math_operations import Calculator


class Uncorrelation_manager:
    def __init__(
        self, logger, cache_manager, bot, chat_id, interval, exchanges, spread
    ):
        self.logger = logger
        self.cache_manager = cache_manager
        self.uncorrelations_list = []
        self.calculator = Calculator()
        self.bot = bot
        self.chat_id = chat_id
        self.interval = interval
        self.exchanges = exchanges
        self.conf_spread = spread
        self.exchanges_data = {}
        self.spreads = {}
        self.valid_spreads = {}
        self.best_spreads = {}
        self.positions_symbols_pair = []
        self.valid_fundings = {}
        self.ready_fundings = {}
        self.uncorrelation_dict = {}
        self.uncorrelations_event = asyncio.Event()


    async def start_work(self, sockets_event, funding_queue, sockets_data, get_fundings):

        await sockets_event.wait()

        self.exchanges_data.clear()
        self.spreads.clear()
        self.valid_spreads.clear()
        
        while True:
            # Собираем все символы и их прайсы в один ебаный self.exchanges_data
            await self._collect_symbols_with_prices_to_dict(sockets_data=sockets_data)

            # получаем спреды между всеми биржами символов в self.spreads = {}
            await self._get_spreads()

            # пиздим валидные спреды в self.best_spreads = {}
            await self._get_valid_spreads()  # {'KNCUSDT': [('binance', 'bybit', 0.5438859714928758),
                                             #              ('binance', 'bitget', 0.5438859714928758)]}
                                             # {'AVLUSDT': [('bitget', 'bybit', 0.8955223880597022)],
                                             #  'KNCUSDT': [('binance', 'bybit', 0.5438859714928758),
                                             #              ('binance', 'bitget', 0.5438859714928758)]}


            # и теперь лучшие спреды
            to_funding_spreads = await self._get_best_spreads()   # {'BANANAS31USDT': ('binance', 'bitget', 0.6283176253927067),
                                                                  #  'KNCUSDT': ('bitget', 'binance', -0.7030527289546763),
                                                                  #  'NTRNUSDT': ('bybit', 'bitget', 0.5020080321285145)}

            await self._get_fundings_for_best_spreads(funding_queue, to_funding_spreads, get_fundings)

            await self._get_ready_funding_pair()

            symbols_with_prices_and_fundings = self.build_symbols_with_prices_and_fundings(
                self.best_spreads,
                self.ready_fundings,
                self.exchanges_data
            )


            uncorrelations = await self._get_uncorrelations(symbols_with_prices_and_fundings)
            self.uncorrelations_list = uncorrelations

            if self.uncorrelations_list:
                self.uncorrelations_event.set()

            await asyncio.sleep(0.5)

    async def _collect_symbols_with_prices_to_dict(self, sockets_data):
        symbols_set = await self.cache_manager.get_all_symbols()
        sockets_data_dict = sockets_data()

        for symbol in symbols_set:
            if symbol not in sockets_data_dict:
                continue

            for exchange in self.exchanges:
                price_info = sockets_data_dict[symbol].get(exchange)
                if price_info is None:
                    continue

                self.exchanges_data.setdefault(symbol, {})[exchange] = {
                    'price': price_info['price'],
                    'time': price_info.get('time', time.time())
                }



    async def _get_spreads(self):
        for symbol, exchanges in self.exchanges_data.items():

            for exchange_1, exchange_2 in combinations(exchanges, 2):

                try:

                    price1 = float(exchanges[exchange_1]['price'])
                    price2 = float(exchanges[exchange_2]['price'])
                except Exception as error:
                    self.logger.error(f'[UNCORRLEATION SYSTEM] Ошибка прайсов - {symbol}, {error}')
                    print(f'{symbol}, exchange - {exchanges[exchange_1]}, data - {exchanges[exchange_1]['price']}')
                    print(f'{symbol}, exchange - {exchanges[exchange_2]}, data - {exchanges[exchange_2]['price']}')

                if price2 == 0 or price1 == 0:
                    continue

                spread = self.calculator.calc_spread(price1, price2)
                self.spreads.setdefault(symbol, {})[f'{exchange_1}-{exchange_2}'] = spread

    async def _get_valid_spreads(self):
        self.valid_spreads.clear()
        active_pairs = await self.cache_manager.get_active_position_symbol_pairs() or []

        for symbol, data in self.spreads.items():
            for stock_pair, spread_value in sorted(data.items(), key=lambda x: -abs(x[1])):
                exch1, exch2 = stock_pair.split('-')
                key = tuple(sorted([symbol, exch1, exch2]))

                if abs(spread_value) >= self.conf_spread or key in active_pairs:
                    self.valid_spreads.setdefault(symbol, []).append((exch1, exch2, spread_value))


    async def _get_best_spreads(self):
        self.best_spreads.clear()
        to_return = {}
        self.positions_symbols_pair = await self.cache_manager.get_active_position_symbol_pairs() or []

        for symbol, spreads in self.valid_spreads.items():

            exch1, exch2, spread_value = spreads[0]
            
            key = tuple(sorted([symbol, exch1, exch2]))
            
            if key in self.positions_symbols_pair:
                best = (exch1, exch2, spread_value)
            else:
                best = max(spreads, key=lambda x: abs(x[2]))
            
            self.best_spreads[symbol] = best
            to_return[symbol] = best
            
            
        return to_return

    async def _get_fundings_for_best_spreads(self, funding_queue, best_spreads_data, get_fundings):

        keys_list = []

        for symbol, (ex1, ex2, spread) in best_spreads_data.items():
            for exch in (ex1, ex2):
                key = (symbol, exch)
                keys_list.append(key)
        
        ready_fundings_from_funding_manager = await get_fundings(keys_list)

        if ready_fundings_from_funding_manager:
        
            for key, data in ready_fundings_from_funding_manager.items():
                symbol, exchange = key
                funding = data['funding']
                next_funding_time = data['next_funding_time']

                self.valid_fundings.setdefault(symbol, {})[exchange] = {
                    'funding': funding,
                    'next_funding_time': next_funding_time
                }
    
    async def _get_ready_funding_pair(self):
        """
        Фильтрует те символы, у которых есть фандинги с обеих бирж best_spreads.
        """
        self.ready_fundings.clear()  # Очищаем, чтобы не копилось старое дерьмо

        for symbol, (ex1, ex2, _) in self.best_spreads.items():
            symbol_fundings = self.valid_fundings.get(symbol)
            if not symbol_fundings:
                continue

            if ex1 in symbol_fundings and ex2 in symbol_fundings:
                self.ready_fundings[symbol] = {
                    ex1: symbol_fundings[ex1],
                    ex2: symbol_fundings[ex2],
                }

    def build_symbols_with_prices_and_fundings(self, best_spreads, ready_fundings, exchanges_data):
        result = {}

        for symbol, (ex1, ex2, _) in best_spreads.items():
            funding_data = ready_fundings.get(symbol)
            if not funding_data:
                continue

            result[symbol] = {}

            for exch in (ex1, ex2):
                exchange_funding = funding_data.get(exch)
                price_data = exchanges_data.get(symbol, {}).get(exch)

                if not exchange_funding or not price_data:
                    continue

                result[symbol][exch] = {
                    'price': float(price_data.get('price', 0)),
                    'time': int(price_data.get('time', 0)),
                    'funding': exchange_funding.get('funding'),
                    'next_funding_time': exchange_funding.get('next_funding_time')
                }

        return result


    async def _get_uncorrelations(self, main_dict):

        self.uncorrelation_dict = {}
        uncorrelation_dict = {}

        for symbol, exchanges in main_dict.items():
            exchange_names = list(exchanges.keys())

            if len(exchange_names) < 2:
                continue

            best_uncorrelation = None
            best_pair = None
            best_diff = -float("inf")

            for ex1, ex2 in combinations(exchange_names, 2):
                data1 = exchanges[ex1]
                data2 = exchanges[ex2]

                if not data1 or not data2:
                    continue

                if 'price' not in data1 or 'funding' not in data1:
                    continue
                if 'price' not in data2 or 'funding' not in data2:
                    continue

                stock1 = {
                    'price': data1['price'],
                    'funding': data1['funding'],
                    'next_funding_time': data1.get('next_funding_time'),
                    'time': data1.get('time')
                }

                stock2 = {
                    'price': data2['price'],
                    'funding': data2['funding'],
                    'next_funding_time': data2.get('next_funding_time'),
                    'time': data2.get('time')
                }

                result = self.calculator.calc_uncorrelation(stock1, stock2, ex1, ex2)

                if result and result.get('difference', 0) > best_diff:
                    best_diff = result['difference']
                    best_uncorrelation = result
                    best_pair = (ex1, ex2)

            if best_uncorrelation:
                uncorrelation_dict[symbol] = best_uncorrelation

        self.uncorrelation_dict = uncorrelation_dict
        await self.cache_manager.set_uncor(uncorrelation_dict)
        return uncorrelation_dict

    
    def get_uncorrelations(self):
        if self.uncorrelation_dict:
            return self.uncorrelation_dict
        else:
            return None

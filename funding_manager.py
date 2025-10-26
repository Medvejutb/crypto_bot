import asyncio
import time
from pprint import pprint

class Funding_manager:
    def __init__(self, logger, cache_manager, funding_funcs, time_live_funding):
        self.logger = logger
        self.cache_manager = cache_manager
        self.funding_funcs = funding_funcs
        self.time_live_funding = time_live_funding
        self.exchanges_fundings_only_api = ['binance', 'okx', 'bybit'] # биржи, которые дают фандинги только по апи
        self.blacklist_for_symbols_with_stocks = {} # {
                                                     #    ('DOGEUSDT', 'bybit'): 'unsupported',
                                                     #    ('MOVRUSDT', 'okx'): 'none_funding',
                                                     #    ('PIZDAUSDT', 'binance'): 'ok',
                                                     #}
        self.last_update_time_funding = {
            exchange: {} for exchange in self.exchanges_fundings_only_api # держит время, когда был получен фандинг
            }
        self.next_time_to_update_funding = {
            exchange: {} for exchange in self.exchanges_fundings_only_api # держит время, когда фандинг должен обновиться
            }
    
    async def get_fundings_for_cur_symbols_with_stocks(self, stock_symbol_list: list):

        ready_fundings = {}

        for item in stock_symbol_list:
            key = item
            symbol, exchange = item

            status = self.blacklist_for_symbols_with_stocks.get(key)
            if status == 'unsupported':
                continue
            funding_from_cache = await self.cache_manager.get_funding(symbol, exchange)
            if funding_from_cache == 'unsupported':
                self.blacklist_for_symbols_with_stocks[key] = 'unsupported'
                continue
            try:
                if funding_from_cache is None:
                    # self.logger.debug(f'[FUNDING] Обработка {symbol} с {exchange}')
                    func = self.funding_funcs[exchange]
                    funding_data = await func(symbol)
                    funding = funding_data[symbol]['funding']
                    next_funding_time = funding_data[symbol]['next_funding_time']
                    # self.logger.debug(f'[FUNDING] Данные получены c {exchange} - {symbol}: {funding_data}')
                    await self.cache_manager.set_funding(symbol, exchange, funding, next_funding_time)           
                    self.blacklist_for_symbols_with_stocks[key] = 'ok'
                else:
                    funding = funding_from_cache
                    next_funding_time = await self.cache_manager.get_next_funding_time(symbol, exchange)
            except TypeError as error:
                self.logger.error(f'[FUNDING SYSTEM] {symbol}')
            except Exception as error:
                self.logger.error(f'[FUNDING SYSTEM] {symbol}')
            ready_fundings[key] = {
                'funding': funding,
                'next_funding_time': next_funding_time
            }

        # if ready_fundings:
        #     pprint(ready_fundings)
        return ready_fundings




















    async def start_work(self, funding_queue):
        while True:
            try:
                data = await funding_queue.get()
            except Exception as e:
                self.logger.exception(f'[FUNDING SYSTEM] Ошибка async очереди {e}')
            try:
                if data:
                    symbol = data['symbol']
                    exchange = data['exchange']
                    if exchange in self.exchanges_fundings_only_api:

                        if not self.last_update_time_funding[exchange][symbol] or time.time() > self.next_time_to_update_funding[exchange][symbol]:

                            self.logger.debug(f'[FUNDING] Обработка {symbol} с {exchange}')
                            func = self.funding_funcs[exchange]
                            funding_data = await func(symbol)
                            self.logger.debug(f'[FUNDING] Данные получены c {exchange} - {symbol}: {funding_data}')

                            self.last_update_time_funding[exchange][symbol] = time.time()

                            if funding_data and symbol in funding_data:

                                funding = funding_data[symbol]['funding']
                                next_funding_time = funding_data[symbol]['next_funding_time']
                                await self.cache_manager.set_funding(
                                    symbol,
                                    exchange,
                                    funding,
                                    next_funding_time
                                )
                            else:
                                self.logger.warning(f'[FUNDING SYSTEM] Нет данных для {symbol} на {exchange}')

                        
                await asyncio.sleep(0.5)

            except Exception as error:
                self.logger.exception(f'[FUNDING SYSTEM] Ошибка обработки данных {error}')

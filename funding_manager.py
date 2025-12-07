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
    
    async def get_fundings(self, stock_symbol_list: list):

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
                self.logger.error(f'[FUNDING SYSTEM] {symbol} - {error}')
            except Exception as error:
                self.logger.error(f'[FUNDING SYSTEM] {symbol} - {error}')
            ready_fundings[key] = {
                'funding': funding,
                'next_funding_time': next_funding_time
            }

        return ready_fundings

    def is_fundings_times_same(self, next_time1, next_time2) -> bool:
        if not next_time1 == next_time2:
            return False
        return True
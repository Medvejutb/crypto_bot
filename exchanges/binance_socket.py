import asyncio
import os
import aiohttp
import websockets
import requests
import json


class WS_binance:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.symbols = []
        self.data = {'stock': 'binance'}
        self.ready = False
        self.url_4_prices = None
        self.connection = False
        self.url_4_fundings = 'https://fapi.binance.com/fapi/v1/premiumIndex'
        self.ready_event = asyncio.Event()

    def load_symbols(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(dir_path, 'binance_symbols.json')
        with open(path, 'r') as file:
            self.symbols = json.load(file)
        self.url_4_prices = f"wss://stream.binance.com:9443/stream?streams={'/'.join([s.lower() + '@ticker' for s in self.symbols])}"

    async def start_socket(self):
        self.load_symbols()
        while True:
            try:
                async with websockets.connect(self.url_4_prices) as websocket:
                    self.connection = True
                    self.logger.debug('[BINANCE SYSTEM] Соединение уставнолено')
                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        data = message['data']
                        self.data[data['s']] = {
                            'price': float(data['c']),
                            'funding': None,
                            'next_funding_time': None,
                            'time': int(data['E'])
                        }

                        if not self.ready and len(self.data) > 30:
                            self.logger.success('[BINANCE SYSTEM] Данных достаточно. Биржа готова.')
                            self.ready = True
                            self.ready_event.set()
                        
                        await self.cache_manager.set_price(data['s'], 'binance', float(data['c']))

            except Exception as error:
                self.connection = False
                self.logger.error(f'[BINANCE SYSTEM] Ошибка в сокете: {error}. Переподключаюсь через 5 секунд...')
                await asyncio.sleep(5)

    def check_connection(self):
        if self.connection:
            return True
        else:
            return None

    def get_prices_data(self):
        return self.data

    def _check_WTF(self, msg):
        pass

    async def get_funding_4_cur_symbols(self, symbols_list) -> dict:
        self.logger.debug('[BINANCE SYSTEM] Прямой сбор фандингов с API без кэша')
        funding_dict = {}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url_4_fundings) as response:
                    data = await response.json()

                    funding_dict = {}

                    for item in data:
                        symbol = item.get('symbol')
                        if symbol in symbols_list:
                            funding = float(item.get('lastFundingRate', 0)) * 100
                            next_funding_time = int(item.get('nextFundingTime', 0))

                            funding_dict[symbol] = {
                                'funding': funding,
                                'next_funding_time': next_funding_time,
                            }

                    return funding_dict

        except Exception as error:
            self.logger.error(f'[BINANCE ERROR] Ошибка при запросе фандингов с API\nОшибка - {error}')






def get_coins_with_status_TRADING() -> list:
    url_4_symbol = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    response = requests.get(url_4_symbol)
    data = response.json()
    coins_list = []

    for item in data['symbols']:
        if (item.get('status') == 'TRADING'
            and item.get('contractType') == 'PERPETUAL'
            and item.get('quoteAsset') == 'USDT'
        ):
            coins_list.append(item.get('pair'))

    with open('binance_symbols.json', 'w') as file:
        json.dump(coins_list, file, indent=4, ensure_ascii=False)





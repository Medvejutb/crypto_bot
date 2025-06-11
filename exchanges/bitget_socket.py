import asyncio
import aiohttp
import websockets
import json

class WS_bitget:
    def __init__(self):
        self.symbols = []
        self.data = {'stock': 'bitget'}
        self.ready = False
        self.url_4_prices = 'wss://ws.bitget.com/mix/v1/stream'
        self.url_4_symbols = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES' #Символы и их фандинги
        self.connection = False


    async def start_socket(self):
        await self.get_symbols()
        subscribe_settings = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "mc",
                    "channel": "ticker",
                    "instId": symbol
                } for symbol in self.symbols
            ]
        }

        while True:
            try:
                async with websockets.connect(self.url_4_prices) as websocket:
                    self.connection = True
                    print('[BITGET SYSTEM] Соединение установлено')
                    await websocket.send(json.dumps(subscribe_settings))
                    print('[BITGET SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        #===============================================
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)

    async def get_symbols(self):
        while True:
            try:
                print('[BITGET] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()

                        self.symbols = []

                        for item in data['data']:
                            symbol = item.get('symbol')
                            if float(item.get('lastPr')) > 0:
                                price = item.get('lastPr')
                            else:
                                continue
                            funding = float(item.get('fundingRate')) * 100
                            time = item.get('ts')
                            next_funding_time = None

                            self.symbols.append(symbol)
                            self.data[symbol] = {
                                'price': price,
                                'funding': funding,
                                'next_funding_time': next_funding_time,
                                'time': time
                            }
                return
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка при сборе символов - {error}. Через 5 сек заново')
                await asyncio.sleep(5)

    def check_ready(self) -> bool:
        if len(self.data) <= 30:
            return False
        self.ready = True
        return True

    def get_prices_data(self):
        return self.data

    async def get_funding_4_cur_symbols(self, symbols_list) -> dict:
        while True:
            try:
                print('[BITGET SYSTEM] Сбор фандингов')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()

                        funding_dict = {}

                        for item in data['data']:
                            symbol = item.get('symbol')
                            if symbol in symbols_list:
                                funding = float(item.get('fundingRate')) * 100
                                next_funding_time = None

                                funding_dict[symbol] = {
                                    'funding': funding,
                                    'next_funding_time': next_funding_time,
                                }
                        return funding_dict
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка при сборе фандингов\nОшибка - {error}')



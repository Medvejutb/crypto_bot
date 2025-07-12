import websockets
import asyncio
import aiohttp
import json
from utils.views import Logging_manager
from cache_manager import Cache_manager

cache_manager = Cache_manager()
logger = Logging_manager.get_logger()

class WS_okx:
    def __init__(self):
        self.symbols = []
        self.instId_list = []
        self.data = {'stock': 'okx'}
        self.ready = False
        self.url_4_symbols = 'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
        self.url_4_prices = 'wss://ws.okx.com:8443/ws/v5/public'
        self.url_4_fundings = 'https://www.okx.com/api/v5/public/funding-rate?instId='
        self.connection = False

    async def start_socket(self):
        await self.get_symbols()

        while True:
            try:
                async with websockets.connect(
                        self.url_4_prices,
                        ping_interval=25,
                        ping_timeout = 10
                ) as websocket:
                    self.connection = True
                    for i in range(0, len(self.instId_list), 30):
                        chunk = self.instId_list[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [
                                {
                                    "channel": "tickers",
                                    "instId": instid
                                } for instid in chunk
                            ]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)
                    logger.debug('[OKX SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)

                        if 'event' in message or 'data' not in message:
                            continue

                        data = message['data'][0]
                        symbol = data['instId'].replace('-', '').replace('SWAP', '')

                        self.data[symbol]['price'] = data.get('last')
                        self.data[symbol]['time'] = data.get('ts')


            except Exception as error:
                logger.error(f'[OKX ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)




    async def get_symbols(self):
        self.instId_list = []
        self.symbols = []
        while True:
            try:
                logger.debug('[OKX] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:

                        data = await response.json()
                        data = data.get('data')

                        for item in data:
                            if item['settleCcy'] == 'USDT' and item['state'] == 'live' and item['ctType'] == 'linear':
                                self.instId_list.append(item.get('instId'))

                                symbol = item.get('uly').replace('-', '')
                                funding = None
                                price = None
                                time = None
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
                logger.error(f'[OKX ERROR] Произошла ошибка при сборе символов - {error}. Через 5 сек заново')
                await asyncio.sleep(5)

    def check_ready(self) -> bool:
        if len(self.data) <= 30:
            return False
        self.ready = True
        return True

    def get_prices_data(self):
        return self.data

    async def get_funding_4_cur_symbols(self, symbols_list) -> dict:
        logger.debug('[OKX SYSTEM] Сбор фандингов')
        while True:

            missing_symbols = [symbol for symbol in symbols_list if cache_manager.get_symbol_funding_data(symbol, 'okx') is None or cache_manager.is_funding_expired(symbol, 'okx')]
            funding_dict = {}

            if missing_symbols:
                
                try:
                    async with aiohttp.ClientSession() as session:

                        for symbol in missing_symbols:
                            
                            instId = symbol.removesuffix('USDT')+'-USDT-SWAP'
                            url = self.url_4_fundings+instId
                            async with session.get(url) as response:

                                data = await response.json()

                                if not data.get('data') or not data.get('code') == '0':
                                    continue
                                
                                logger.debug('=================REST API OKX')
                                funding = float(data['data'][0].get('fundingRate')) * 100
                                next_funding_time = int(data['data'][0].get('nextFundingTime'))

                                cache_manager.set_symbol_data(
                                    symbol=symbol,
                                    stock='okx',
                                    funding=funding,
                                    next_time=next_funding_time,
                                )

                except Exception as error:
                    logger.error(f'[OKX ERROR] Произошла ошибка при сборе фандингов c REST API для {symbol}\nОшибка - {error}')
            
            for symbol in symbols_list:
                cached = cache_manager.get_symbol_funding_data(symbol, 'okx')
                if  cached:
                    funding_dict[symbol] = {
                        'funding': cached.get('funding'),
                        'next_funding_time': cached.get('next_time'),
                        }
            return funding_dict
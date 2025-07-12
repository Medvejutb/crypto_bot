import asyncio
import aiohttp
import websockets
import json
from utils.views import Logging_manager
from cache_manager import Cache_manager

cache_manager = Cache_manager()
logger = Logging_manager.get_logger()

class WS_bitget:
    def __init__(self):
        self.symbols = []
        self.data = {'stock': 'bitget'}
        self.ready = False
        self.url_4_prices = 'wss://ws.bitget.com/v2/ws/public'
        self.url_4_symbols = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        self.url_4_symbols_next_funding = 'https://api.bitget.com/api/v2/mix/market/funding-time?symbol='
        self.connection = False

    async def ping_loop(self, websocket):
        while True:
            try:
                await websocket.send(json.dumps({"op": "ping"}))
                await asyncio.sleep(20)
            except Exception as e:
                logger.error(f"[BITGET PING ERROR] {e}")
                return

    async def start_socket(self):
        await self.get_symbols()

        subscribe_settings = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": symbol
                } for symbol in self.symbols
            ]
        }

        while True:
            try:
                async with websockets.connect(
                        self.url_4_prices,
                        ping_interval=None,
                        close_timeout=5
                ) as websocket:
                    
                    ping_task = asyncio.create_task(self.ping_loop(websocket))

                    self.connection = True
                    logger.debug('[BITGET SYSTEM] Соединение установлено')

                    await websocket.send(json.dumps(subscribe_settings))
                    logger.debug('[BITGET SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        if message.get('event') == 'subscribe':
                            continue
                        if message.get('action') == 'snapshot':
                            data = message.get('data')[0]

                            symbol = data.get('instId')
                            last_price = data.get('lastPr')
                            funding = data.get('fundingRate')
                            next_funding_time = data.get('nextFundingTime')
                            time = data.get('ts')

                            self.data[symbol] = {
                                'price': last_price,
                                'funding': float(funding) * 100,
                                'next_funding_time': next_funding_time,
                                'time': time
                            }
            except Exception as error:
                logger.error(f'[BITGET ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 2 секунды')
                await asyncio.sleep(2)
            finally:
                ping_task.cancel()

    async def get_symbols(self):
        while True:
            try:
                logger.debug('[BITGET] Сбор символов из REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()

                        self.symbols = []

                        for item in data['data']:
                            symbol = item.get('symbol')
                            last_price = item.get('lastPr')

                            if symbol and last_price and float(last_price) > 0:
                                self.symbols.append(symbol)

                return
            except Exception as error:
                logger.error(f'[BITGET ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)

    def check_ready(self) -> bool:
        if len(self.data) <= 30:
            return False
        self.ready = True
        return True

    def get_prices_data(self):
        return self.data

    async def get_funding_4_cur_symbols(self, symbols_list) -> dict:
        logger.debug('[BITGET SYSTEM] Сбор фандингов')

        funding_dict = {}

        # Получаем список символов, которых ещё нет в кэше
        missing_symbols = [symbol for symbol in symbols_list if cache_manager.get_symbol_funding_data(symbol, 'bitget') is None or cache_manager.is_funding_expired(symbol, 'bitget')]

        # Если есть чего забирать — идём в API
        if missing_symbols:
            logger.debug('===========================REST API BITGET')
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url_4_symbols) as response:
                    data = await response.json()

                    for item in data['data']:
                        item_symbol = item.get('symbol')

                        if item_symbol in missing_symbols:
                            funding = float(item.get('fundingRate')) * 100
                            next_funding_time = await self.get_next_funding(item_symbol)

                            cache_manager.set_symbol_data(
                                symbol=item_symbol,
                                stock='bitget',
                                funding=funding,
                                next_time=next_funding_time,
                            )

        # Теперь собираем всё в funding_dict из кэша
        for symbol in symbols_list:
            cached = cache_manager.get_symbol_funding_data(symbol, 'bitget')
            if cached:
                funding_dict[symbol] = {
                    'funding': float(cached.get('funding')),
                    'next_funding_time': int(cached.get('next_time'))
                }

        return funding_dict

    async def get_next_funding(self, symbol) -> int:
        url = f'https://api.bitget.com/api/v2/mix/market/funding-time?symbol={symbol}&productType=usdt-futures'
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        data = await response.json()
                        data = data['data'][0]
                        return int(data['nextFundingTime'])
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка при сборе фандинга - {error}. Через 5 сек заново')
                await asyncio.sleep(5)



"""async def main():
    suka = WS_bitget()
    await suka.start_socket()

asyncio.run(main())"""
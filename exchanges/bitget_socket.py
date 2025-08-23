import asyncio
import aiohttp
import websockets
import json



class WS_bitget:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
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
                self.logger.error(f"[BITGET PING ERROR] {e}")
                return

    async def start_socket(self,queue):
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
                    self.logger.debug('[BITGET SYSTEM] Соединение установлено')

                    await websocket.send(json.dumps(subscribe_settings))
                    self.logger.debug('[BITGET SOCKET] Подписка отправлена')

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

                            await self.cache_manager.set_price(symbol, 'bitget', last_price)
                            await self.cache_manager.set_funding(symbol, 'bitget', float(funding)*100, next_funding_time)

                            if not self.ready and len(self.data) > 30:
                                self.logger.success('[BITGET SYSTEM] Данных достаточно. Биржа готова.')
                                self.ready = True
                                self.ready_event.set()

            except Exception as error:
                self.logger.error(f'[BITGET ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 2 секунды')
                await asyncio.sleep(2)
            finally:
                ping_task.cancel()

    async def get_symbols(self):
        while True:
            try:
                self.logger.debug('[BITGET] Сбор символов из REST API')
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
                self.logger.error(f'[BITGET ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)
                

    def get_prices_data(self):
        return self.data


    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """
        Получает funding rate и next funding time по одному символу с Bitget API.
        Без кэша. Только жёсткий API.

        :param symbol: Тикер символа (например, BTCUSDT)
        :return: dict с ключами: funding, next_funding_time
        """
        self.logger.debug(f'[BITGET SYSTEM] Получение фандинга по {symbol}')
        funding_url = f'https://api.bitget.com/api/v2/mix/market/current-fundRate?symbol={symbol}&productType=usdt-futures'
        time_url = f'https://api.bitget.com/api/v2/mix/market/funding-time?symbol={symbol}&productType=usdt-futures'

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(funding_url) as funding_response:
                        funding_data = await funding_response.json()
                        funding = float(funding_data['data']['fundingRate']) * 100

                    async with session.get(time_url) as time_response:
                        time_data = await time_response.json()
                        next_time = int(time_data['data'][0]['nextFundingTime'])
                        
                    funding_dict = {}
                    funding_dict[symbol] = {
                        'funding': funding,
                        'next_funding_time': next_time,
                    }

                    return funding_dict
            except Exception as error:
                self.logger.error(f'[BITGET ERROR] Ошибка при получении фандинга по {symbol} - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)



"""async def main():
    suka = WS_bitget()
    await suka.start_socket()

asyncio.run(main())"""
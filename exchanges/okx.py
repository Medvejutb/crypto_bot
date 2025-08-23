import websockets
import asyncio
import aiohttp
import json

class WS_okx:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
        self.symbols = []
        self.instId_list = []
        self.data = {'stock': 'okx'}
        self.ready = False
        self.url_4_symbols = 'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
        self.url_4_prices = 'wss://ws.okx.com:8443/ws/v5/public'
        self.connection = False
        self.instId_map = {}


    async def start_socket(self, queue):
        await self.get_symbols()

        while True:
            try:
                async with websockets.connect(self.url_4_prices, ping_interval=25, ping_timeout=10) as websocket:
                    self.connection = True

                    # Подписка на чанки по 30 инструментов
                    for i in range(0, len(self.instId_list), 30):
                        chunk = self.instId_list[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [{"channel": "tickers", "instId": instid} for instid in chunk]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)

                    self.logger.debug('[OKX SOCKET] Подписка отправлена')

                    while True:
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=15)
                        except asyncio.TimeoutError:
                            self.logger.warning('[OKX] recv таймаут — повтор чтения')
                            continue

                        try:
                            message = json.loads(msg)
                        except Exception as e:
                            self.logger.warning(f'[OKX] JSON ошибка: {e} — raw: {msg}')
                            continue

                        if 'event' in message or 'data' not in message:
                            continue

                        data = message['data'][0]
                        instId = data['instId']
                        symbol = self.instId_map.get(instId)

                        if not symbol:
                            continue

                        price = data.get('last')
                        ts = data.get('ts')

                        if not price or not ts:
                            continue

                        self.data[symbol] = self.data.get(symbol, {})
                        self.data[symbol]['price'] = float(price)
                        self.data[symbol]['time'] = int(ts)
                        self.data[symbol]['funding'] = None
                        self.data[symbol]['next_funding_time'] = None

                        await self.cache_manager.set_price(symbol, 'okx', price)

                        if not self.ready and len(self.data) > 30:
                            self.logger.success('[OKX SYSTEM] Биржа готова.')
                            self.ready = True
                            self.ready_event.set()

            except Exception as error:
                self.logger.error(f'[OKX ERROR] Сокет сдох - {error}. Реконнект через 5 секунд')
                await asyncio.sleep(5)


    async def get_symbols(self):
        self.instId_list = []
        self.symbols = []
        while True:
            try:
                self.logger.debug('[OKX] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()
                        for item in data.get('data', []):
                            if item['settleCcy'] == 'USDT' and item['state'] == 'live' and item['ctType'] == 'linear':
                                instId = item.get('instId')
                                symbol = item.get('uly').replace('-', '')
                                self.instId_map[instId] = symbol

                                self.instId_list.append(instId)
                                self.symbols.append(symbol)

                                self.data[symbol] = {
                                    'price': None,
                                    'funding': None,
                                    'next_funding_time': None,
                                    'time': None
                                }
                        return
            except Exception as error:
                self.logger.error(f'[OKX ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)


    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """ Получает funding rate и время следующего фандинга по символу """
        try:
            instId = symbol.removesuffix('USDT') + '-USDT-SWAP'
            url = f'https://www.okx.com/api/v5/public/funding-rate?instId={instId}'

            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
                    if data.get('code') != '0' or not data.get('data'):
                        return {}

                    funding = float(data['data'][0]['fundingRate']) * 100
                    next_time = int(data['data'][0]['nextFundingTime'])
                    
                    funding_dict = {}
                    funding_dict[symbol] = {
                        'funding': funding,
                        'next_funding_time': next_time,
                    }

                    return funding_dict

        except Exception as error:
            self.logger.error(f'[OKX ERROR] REST фандинг по {symbol} сдох: {error}')
            return {}

    def get_prices_data(self):
        return self.data

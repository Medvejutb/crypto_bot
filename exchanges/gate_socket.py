import asyncio
import websockets
import json
import aiohttp
from pathlib import Path
import time as pytime

class WS_gate:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
        self.symbols = []
        self.symbols_data = {'stock': 'gate'}
        self.ready = False
        self.url_4_price = 'wss://api.gateio.ws/ws/v4/'
        self.connection = False

    def get_symbols(self):
        self.logger.debug('[GATE] Сбор символов из binance_symbols.json')
        json_path = Path(__file__).resolve().parent.parent / 'exchanges' / 'binance_symbols.json'
        with open(json_path, 'r') as file:
            self.symbols = ['_'.join([symbol.split('USDT')[0], 'USDT']) for symbol in json.load(file)]

    async def get_gate_available_symbols(self):
        url = "https://api.gate.io/api/v4/futures/usdt/contracts"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.warning(f"[GATE REST] Не удалось получить список контрактов: {response.status}")
                        return []
                    data = await response.json()
                    return [item["name"] for item in data]
        except Exception as e:
            self.logger.error(f"[GATE REST ERROR] Ошибка при получении списка символов: {e}")
            return []

    async def start_socket(self, queue):
        self.get_symbols()
        valid_symbols = await self.get_gate_available_symbols()
        self.symbols = [s for s in self.symbols if s in valid_symbols]
        self.logger.debug(f'[GATE] Валидных символов для подписки: {len(self.symbols)}')

        while True:
            try:
                async with websockets.connect(self.url_4_price) as websocket:
                    self.connection = True
                    self.logger.debug('[GATE SYSTEM] Соединение установлено')

                    for i in range(0, len(self.symbols), 30):
                        chunk = self.symbols[i:i + 30]
                        subscribe_settings = {
                            "id": int(pytime.time() * 1000) + i,
                            "time": int(pytime.time()),
                            "channel": "futures.tickers",
                            "event": "subscribe",
                            "payload": chunk
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)
                    self.logger.debug('[GATE SOCKET] Подписка отправлена')

                    while True:
                        try:
                            msg = await websocket.recv()
                            message = json.loads(msg)

                            if message.get('event') != 'update':
                                continue

                            result = message.get('result')
                            if not isinstance(result, list) or not result:
                                continue

                            data = result[0]
                            symbol = data.get('contract')
                            price = data.get('last')

                            if not symbol or price is None:
                                continue

                            self.symbols_data[symbol] = self.symbols_data.get(symbol, {})
                            self.symbols_data[symbol]['price'] = float(price)
                            self.symbols_data[symbol]['time'] = int(pytime.time() * 1000)

                            await self.cache_manager.set_price(symbol, 'gate', price)

                            await queue.put({
                                'type': 'price_update',
                                'exchange': 'gate',
                                'symbol': symbol,
                                'price': float(price),
                                'timestamp': self.symbols_data[symbol]['time']
                            })

                            if not self.ready and len(self.symbols_data) > 30:
                                self.logger.success('[GATE SYSTEM] Биржа готова.')
                                self.ready = True
                                self.ready_event.set()
                        except Exception as msg_error:
                            self.logger.warning(f'[GATE SOCKET] Ошибка обработки сообщения: {msg_error}')
            except Exception as error:
                self.logger.error(f'[GATE ERROR] Сокет сдох - {error}. Реконнект через 5 секунд')
                await asyncio.sleep(5)

    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """ Забирает funding rate и next funding time по символу с REST API """
        try:
            url = f"https://api.gate.io/api/v4/futures/usdt/contracts/{symbol}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.warning(f'[GATE REST] Не смог получить фандинг: {response.status}')
                        return {}

                    data = await response.json()

                    funding_rate = float(data.get('funding_rate', 0)) * 100
                    funding_interval = int(data.get('funding_interval', 0))
                    last_funding_time = int(data.get('last_funding_time', 0))

                    if funding_interval and last_funding_time:
                        next_time = last_funding_time + funding_interval
                    else:
                        next_time = None

                    return {
                        'funding': funding_rate,
                        'next_funding_time': next_time
                    }

        except Exception as e:
            self.logger.error(f'[GATE REST ERROR] Ошибка при получении фандинга для {symbol}: {e}')
            return {}

    def get_prices_data(self):
        return self.symbols_data

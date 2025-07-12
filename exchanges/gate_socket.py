import asyncio
import websockets
import json
from pathlib import Path
import time as pytime
from utils.views import Logging_manager
from cache_manager import Cache_manager

cache_manager = Cache_manager()
logger = Logging_manager.get_logger()

class WS_gate:
    def __init__(self):
        self.symbols = []
        self.symbols_data = {'stock': 'gate'}
        self.ready = False
        self.url_4_price_funding = 'wss://fx-ws.gate.io/v4/ws/usdt'
        self.url_4_price = 'wss://fx-ws.gateio.ws/v4/ws/usdt'
        self.connection = False

    def get_symbols(self):
        logger.debug('[GATE] Сбор символов REST API')
        json_path = Path(__file__).resolve().parent.parent / 'exchanges' / 'binance_symbols.json'
        with open(json_path, 'r') as file:
            self.symbols = ['_'.join([symbol.split('USDT')[0], 'USDT']) for symbol in json.load(file)]



    async def start_socket(self):
        self.get_symbols()

        while True:
            try:
                async with websockets.connect(self.url_4_price) as websocket:
                    self.connection = True
                    logger.debug('[GATE SYSTEM] Соединение установлено')
                    for i in range(0, len(self.symbols), 30):
                        chunk = self.symbols[i:i + 30]
                        subscribe_settings = {
                            "id": int(pytime.time() * 1000) + i,  # уникальный id на каждый чанк
                            "time": int(pytime.time()),  # текущее время
                            "channel": "futures.tickers",
                            "event": "subscribe",
                            "payload": chunk
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)
                    logger.debug('[GATE SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)

                        if message.get('event') == 'subscribe':
                            continue
                        elif message.get('event') == 'update':
                            result = message.get('result')
                            data = result[0]

                            symbol = data.get('contract')
                            price = data.get('last')
                            funding = data.get('funding_rate')
                            next_funding_time = None
                            time = None

                            self.symbols_data[symbol] = {
                                'price': price,
                                'funding': funding,
                                'next_funding_time': next_funding_time,
                                'time': time
                            }


            except Exception as error:
                logger.error(f'[GATE ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)

    def check_ready(self) -> bool:
        if len(self.symbols_data) <= 30:
            return False
        self.ready = True
        return True

    def get_prices_data(self):
        return self.symbols_data

"""async def main():
    suka = WS_gate()
    await suka.start_socket()

asyncio.run(main())"""
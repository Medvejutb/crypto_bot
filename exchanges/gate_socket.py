"""
Сервак АПИ гейта не работает, разработка приостановлена
"""


import asyncio
import websockets
import json
import os
import aiohttp

from pprint import pprint

class WS_gate:
    def __init__(self):
        self.symbols = []
        self.symbols_data = {'stock': 'gate'}
        self.ready = False
        self.url_4_price_funding = 'wss://fx-ws.gate.io/v4/ws/usdt'
        self.url_4_symbols = 'https://api.gate.io/api/v4/futures/usdt/contracts'
        self.connection = False

    async def get_symbols(self):
        while True:
            try:
                print('[GATE] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()
                        self.symbols = [item['name'] for item in data if 'USDT' in item.get('name', '')]
                return
            except Exception as error:
                print(f'[GATE ERROR] Произошла ошибка при сборе символов - {error}. Через 5 сек заново')
                await asyncio.sleep(5)

    async def start_socket(self):
        await self.get_symbols()
        subscribe_settings = {
              "time": 1234567890,
              "channel": "futures.tickers",
              "event": "subscribe",
              "payload": self.symbols,
              "id": 1337
            }

        while True:
            try:
                async with websockets.connect(self.url_4_price_funding) as websocket:
                    self.connection = True
                    print('[GATE SYSTEM] Соединение установлено')
                    await websocket.send(json.dumps(subscribe_settings))
                    print('[GATE SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        pprint(message)
                        if message.get("channel") == "futures.tickers" and message.get("event") == "update":
                            result = message.get("result", {})
                            symbol = result.get("contract")
                            if not symbol:
                                continue

                            try:
                                price = float(result.get("last", 0))
                                funding = float(result.get("funding_rate", 0)) * 100
                                next_funding_time = int(result.get("next_funding_time", 0))
                            except (ValueError, TypeError):
                                continue

                            self.symbols_data[symbol] = {
                                'price': price,
                                'funding': funding,
                                'next_funding_time': next_funding_time,
                                'timestamp': int(message['time']) * 1000
                            }
                            pprint(self.symbols_data)
                        await asyncio.sleep(1)
            except Exception as error:
                print(f'[GATE ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)


async def main():
    suka = WS_gate()
    await suka.start_socket()

asyncio.run(main())
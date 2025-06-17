import asyncio
import aiohttp
import websockets
import json
from pprint import pprint

class WS_bitget:
    def __init__(self):
        self.symbols = []
        self.data = {'stock': 'bitget'}
        self.ready = False
        self.url_4_prices = 'wss://ws.bitget.com/mix/v1/stream'
        self.url_4_symbols = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
        self.url_4_symbols_next_funding = 'https://api.bitget.com/api/v2/mix/market/funding-time?symbol='
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
                async with websockets.connect(
                        self.url_4_prices,
                        ping_interval=None,  # отключаем автоматический
                        close_timeout=5
                ) as websocket:
                    self.connection = True
                    print('[BITGET SYSTEM] Соединение установлено')

                    # Врубаем ручной пинг
                    ping_task = asyncio.create_task(self.manual_ping(websocket))

                    await websocket.send(json.dumps(subscribe_settings))
                    print('[BITGET SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        await asyncio.sleep(1)

            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)
            finally:
                try:
                    ping_task.cancel()
                except:
                    pass


    async def manual_ping(self, websocket):
        while True:
            try:
                await websocket.send(json.dumps({"op": "ping"}))  # Bitget требует именно такой формат
                await asyncio.sleep(15)  # интервал можно настроить, 15 сек — норм
            except Exception as e:
                print(f'[BITGET PING] Ошибка при отправке ping: {e}')
                break  # выйдем из пинга, чтобы основной цикл словил reconnection

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
                                next_funding_time = await self.get_next_funding(symbol)

                                funding_dict[symbol] = {
                                    'funding': funding,
                                    'next_funding_time': next_funding_time,
                                }
                        return funding_dict
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка при сборе фандингов\nОшибка - {error}')

    async def get_next_funding(self, symbol) -> int:
        url = f'https://api.bitget.com/api/v2/mix/market/funding-time?symbol={symbol}&productType=usdt-futures'
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        data = await response.json()
                        data = data['data'][0]
                        return data['nextFundingTime']
            except Exception as error:
                print(f'[BITGET ERROR] Произошла ошибка при сборе фандинга - {error}. Через 5 сек заново')
                await asyncio.sleep(5)



"""async def main():
    suka = WS_bitget()
    await suka.get_next_funding('BTCUSDT')

asyncio.run(main())"""
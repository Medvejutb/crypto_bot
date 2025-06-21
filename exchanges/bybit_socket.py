import asyncio
import websockets
import json
import aiohttp
from utils.views import Logging_manager

logger = Logging_manager.get_logger()

class WS_bybit:
    def __init__(self):
        self.symbols = []
        self.symbols_data = {'stock': 'bybit'}
        self.ready = False
        self.url_4_price_funding = 'wss://stream.bybit.com/v5/public/linear'
        self.url_4_symbols = 'https://api.bybit.com/v5/market/instruments-info?category=linear'
        self.connection = False

    async def get_symbols(self):
        self.symbols = []
        while True:
            try:
                logger.debug('[BYBIT] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:

                        data = await response.json()
                        data = data['result']['list']
                        for item in data:
                            if (item.get('contractType') == 'LinearPerpetual'
                                    and item.get('quoteCoin') == 'USDT'):
                                        self.symbols.append(item.get('symbol'))
                        return
            except Exception as error:
                logger.error(f'[BYBIT ERROR] Произошла ошибка при сборе символов - {error}. Через 5 сек заново')
                await asyncio.sleep(5)

    async def start_socket(self):
        await self.get_symbols()

        while True:
            try:
                async with websockets.connect(self.url_4_price_funding) as websocket:
                    self.connection = True
                    for i in range(0, len(self.symbols), 30):
                        chunk = self.symbols[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [f"tickers.{symbol}" for symbol in chunk]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)
                    logger.debug('[BYBIT SOCKET] Подписка отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        data = message.get('data', {})
                        if message.get('type') == 'snapshot':
                            data = message.get('data', {})

                            symbol = data.get('symbol')
                            funding_raw = data.get('fundingRate')
                            price_raw = data.get('lastPrice')
                            next_funding_raw = data.get('nextFundingTime')
                            timestamp = message.get('ts')

                            if None in (symbol, funding_raw, price_raw, next_funding_raw, timestamp):
                                logger.debug(
                                    f"[BYBIT WARNING] Пропущен snapshot: не хватает данных. symbol={symbol},"
                                    f" funding={funding_raw}, price={price_raw}, next={next_funding_raw}, ts={timestamp}")
                                continue  # хуярим дальше

                            try:
                                funding = float(funding_raw) * 100
                                price = float(price_raw)
                                next_funding_time = int(next_funding_raw)
                                time = int(timestamp)
                            except (ValueError, TypeError) as e:
                                logger.error(f"[BYBIT ERROR] Ошибка приведения типов в snapshot: {e}")
                                continue

                            self.symbols_data[symbol] = {
                                'price': price,
                                'funding': funding,
                                'next_funding_time': next_funding_time,
                                'time': time
                            }

                        elif message.get("type") == "delta":
                            symbol = data.get("symbol")
                            if not symbol:
                                continue

                            if symbol not in self.symbols_data:
                                # ещё нет snapshot'а — нахуй такой delta
                                continue

                            updated = False
                            if 'lastPrice' in data:
                                try:
                                    self.symbols_data[symbol]['price'] = float(data['lastPrice'])
                                    updated = True
                                except:
                                    pass
                            if 'fundingRate' in data:
                                try:
                                    self.symbols_data[symbol]['funding'] = float(data['fundingRate']) * 100
                                    updated = True
                                except:
                                    pass
                            if 'nextFundingTime' in data:
                                try:
                                    self.symbols_data[symbol]['next_funding_time'] = int(data['nextFundingTime'])
                                    updated = True
                                except:
                                    pass
                            if updated:
                                self.symbols_data[symbol]['time'] = int(message['ts'])
            except Exception as error:
                logger.error(f'[BYBIT ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 5 секунд')
                await asyncio.sleep(5)

    async def get_funding_4_cur_symbols(self, symbols_list):
        funding_dict = {}

        for symbol_from_list in symbols_list:
            if symbol_from_list in self.symbols_data:
                funding = self.symbols_data[symbol_from_list]['funding']
                next_funding_time = self.symbols_data[symbol_from_list]['next_funding_time']
                funding_dict[symbol_from_list] = {
                    'funding': funding,
                    'next_funding_time': next_funding_time,
                }

        return funding_dict

    def check_ready(self) -> bool:
        if len(self.symbols_data) <= 30:
            return False
        self.ready = True
        return True

    def get_prices_data(self):
        return self.symbols_data
'''
async def main():
    suka = WS_bybit()
    await suka.start_socket()

asyncio.run(main())'''
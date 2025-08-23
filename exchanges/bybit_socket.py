import asyncio
import websockets
import json
import aiohttp

class WS_bybit:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
        self.symbols = []
        self.symbols_data = {'stock': 'bybit'}
        self.ready = False
        self.url_4_price = 'wss://stream.bybit.com/v5/public/linear'
        self.url_4_symbols = 'https://api.bybit.com/v5/market/instruments-info?category=linear'
        self.connection = False

    async def get_symbols(self):
        self.symbols = []
        while True:
            try:
                self.logger.debug('[BYBIT] Сбор символов REST API')
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url_4_symbols) as response:
                        data = await response.json()
                        data = data['result']['list']
                        for item in data:
                            if item.get('contractType') == 'LinearPerpetual' and item.get('quoteCoin') == 'USDT':
                                self.symbols.append(item.get('symbol'))
                        return
            except Exception as error:
                self.logger.error(f'[BYBIT ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)

    async def start_socket(self, queue):
        await self.get_symbols()

        while True:
            try:
                async with websockets.connect(self.url_4_price) as websocket:
                    self.connection = True

                    for i in range(0, len(self.symbols), 30):
                        chunk = self.symbols[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [f"tickers.{symbol}" for symbol in chunk]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)

                    self.logger.debug('[BYBIT SOCKET] Подписка на цены отправлена')

                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)

                        if message.get('type') not in ('snapshot', 'delta'):
                            continue

                        data = message.get('data', {})
                        symbol = data.get('symbol')
                        if not symbol:
                            continue

                        if 'lastPrice' in data:
                            try:
                                price = float(data['lastPrice'])
                                self.symbols_data[symbol] = self.symbols_data.get(symbol, {})
                                self.symbols_data[symbol]['price'] = price
                                self.symbols_data[symbol]['time'] = int(message['ts'])

                                await self.cache_manager.set_price(symbol, 'bybit', price)

                            except (ValueError, TypeError) as e:
                                self.logger.error(f'[BYBIT ERROR] Ошибка приведения цены: {e}')

                            await queue.put({
                                'type': 'price_update',
                                'exchange': 'bybit',
                                'symbol': symbol,
                                'price': self.symbols_data[symbol]['price'],
                                'timestamp': self.symbols_data[symbol]['time']
                            })

                            if not self.ready and len(self.symbols_data) > 30:
                                self.logger.success('[BYBIT SYSTEM] Биржа готова.')
                                self.ready = True
                                self.ready_event.set()

            except Exception as error:
                self.logger.error(f'[BYBIT ERROR] Сокет сдох - {error}. Реконнект через 5 сек')
                await asyncio.sleep(5)

    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """ Сбор фандинга и времени через REST API """
        try:
            funding_url = f'https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}'
            async with aiohttp.ClientSession() as session:
                async with session.get(funding_url) as resp:
                    data = await resp.json()
                    latest = data['result']['list'][0]

                    funding = float(latest['fundingRate']) * 100
                    last_time = int(latest['fundingRateTimestamp'])
                    next_time = last_time + 8 * 60 * 60 * 1000  # +8 часов

                    funding_dict = {}
                    funding_dict[symbol] = {
                        'funding': funding,
                        'next_funding_time': next_time,
                    }

                    return funding_dict

        except Exception as e:
            self.logger.error(f'[BYBIT ERROR] Не смог получить фандинг по {symbol} - {e}')
            return {}

    def get_prices_data(self):
        return self.symbols_data

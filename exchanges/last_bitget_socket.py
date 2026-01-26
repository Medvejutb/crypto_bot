import asyncio
import aiohttp
import websockets
import json
from decimal import Decimal, ROUND_DOWN
from dotenv import load_dotenv
import hmac
import hashlib
import base64
import time
import os
from pprint import pprint



class WS_bitget:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
        self.symbols = []
        self.data = {'stock': 'bitget'}
        self.ready = False
        self.url_socket = 'wss://ws.bitget.com/v2/ws/public'
        self.api_base_market = 'https://api.bitget.com/api/v2/mix/market/'
        self.url_order = "https://api.bitget.com/api/v2/mix/order/place-order"
        self.connection = False
        self.session = None
        self.symbols_info_4_order = {}
        load_dotenv()
        self.API_KEY = os.getenv('BITGET_API_KEY')
        self.SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
        self.PASSWORD = os.getenv('BITGET_PASSWORD')

    async def ping_loop(self, websocket):
        while True:
            try:
                await websocket.send("ping")
                await asyncio.sleep(20)
            except Exception as e:
                self.logger.error(f"[BITGET PING ERROR] {e}")
                return
    async def open_client(self):
        self.session = aiohttp.ClientSession()

    async def start_socket(self):
        await self.open_client()
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
                        self.url_socket,
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

                        if msg == "pong":
                            continue

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
                if 'ping_task' in locals():
                    ping_task.cancel()

    async def get_symbols(self):
        while True:
            try:
                async with self.session.get(
                    f'{self.api_base_market}tickers?productType=USDT-FUTURES'
                ) as response:
                    data = await response.json()
                    self.symbols = []
                    for item in data['data']:
                        symbol = item.get('symbol')
                        last_price = item.get('lastPr')
                        if symbol and last_price and float(last_price) > 0:
                            self.symbols.append(symbol)

                break
            except Exception as error:
                self.logger.error(f'[BITGET ERROR] Ошибка при сборе символов - {error}. Повтор через 1 сек')
                await asyncio.sleep(1)

        while True:
            try:
                async with self.session.get(

                    f'{self.api_base_market}'
                    f'contracts?productType=USDT-FUTURES'
                    
                    ) as response:
                    raw = await response.json()
                    data = raw['data']
                    for item in data:
                        symbol = item['symbol']
                        self.symbols_info_4_order.setdefault(symbol, item)
                    break
            except Exception as error:
                self.logger.error(f'[BITGET ERROR] Ошибка при сборе инфы символов для ордеров - {error}. Повтор через 1 сек')
                await asyncio.sleep(1)
                

    def get_prices_data(self):
        return self.data


    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """
        TODO:
        Ошибка при получении фандинга по LINEAUSDT - 'NoneType' object is not subscriptable


        Получает funding rate и next funding time по одному символу с Bitget API.
        Без кэша. Только жёсткий API.

        :param symbol: Тикер символа (например, BTCUSDT)
        :return: dict с ключами: funding, next_funding_time
        """
        self.logger.debug(f'[BITGET SYSTEM] Получение фандинга по {symbol}')

        while True:
            try:
                async with self.session.get(

                    f'{self.api_base_market}'
                    f'current-fundRate?symbol={symbol}&productType=usdt-futures'
                    
                    ) as funding_response:

                    funding_data = await funding_response.json()
                    funding = float(funding_data['data']['fundingRate']) * 100
                
                async with self.session.get(

                    f'{self.api_base_market}'
                    f'funding-time?symbol={symbol}&productType=usdt-futures'

                ) as time_response:
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

    def _timestamp(
            self
    ):
        return str(int(time.time() * 1000))

    def convert_usd_to_contracts(
        self,
        symbol: str,
        usd_volume: Decimal,
        price: Decimal,
        ):
        
        usd_volume = Decimal(usd_volume)
        price = Decimal(price)

        if price == 0:
            self.logger.error(
                f'[BITGET ORDER] прайс равен нулю'
            )
            return None

        if symbol not in self.symbols_info_4_order.keys():
            self.logger.info(
                f'[BITGET SYSTEM] Символ {symbol} не торгуется'
            )
            return None
        
        contract_info = self.symbols_info_4_order[symbol]

        base_qty = usd_volume / price

        size_multiplier = Decimal(str(contract_info['sizeMultiplier']))

        base_qty = (base_qty / size_multiplier).to_integral_value(rounding=ROUND_DOWN) * size_multiplier

        volume_place = int(contract_info['volumePlace'])
        quant = Decimal('1').scaleb(-volume_place)

        base_qty = base_qty.quantize(quant, rounding=ROUND_DOWN)

        min_trade = Decimal(contract_info['minTradeNum'])
        min_usdt = Decimal(contract_info['minTradeUSDT'])
        max_qty = Decimal(contract_info['maxMarketOrderQty'])

        if base_qty < min_trade:
            self.logger.error(
                f'[BITGET ORDER] количество базового актива ({base_qty}) < минимального объема сделки ({min_trade})'
            )
            return None

        if base_qty * price < min_usdt:
            self.logger.error(
                f'[BITGET ORDER] сумма сделки в $ ({base_qty * price}) < минимальной ({min_usdt})'
            )
            return None

        if base_qty > max_qty:
            self.logger.error(
                f'[BITGET ORDER] количество базового актива ({base_qty}) > максимального объема сделки ({max_qty})'
            )
            return None
        
        size = format(base_qty, 'f')

        return size

    def _sign(
        self,
        timestamp,
        method,
        request_path,
        body,
        secret_key,
    ):
        pre_sign = f"{timestamp}{method.upper()}{request_path}{body}"
        sign = hmac.new(
            secret_key.encode("utf-8"),
            pre_sign.encode("utf-8"),
            hashlib.sha256).digest()
        return base64.b64encode(sign).decode()
    
    def _headers(
        self,
        method,
        path,
        body,
    ):
        ts = self._timestamp()
        return {
            "ACCESS-KEY": self.API_KEY,
            "ACCESS-SIGN": self._sign(ts, method, path, body, self.SECRET_KEY),
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self.PASSWORD,
            "Content-Type": "application/json",
        }

    async def place_order(
        self,
        symbol: str,
        side: str,
        volume: Decimal,
        price: Decimal,
        posSide: str,
    ):
        try:
            usd_count = volume
            request_path = "/api/v2/mix/order/place-order"
            method = 'POST'

            size = self.convert_usd_to_contracts(
                symbol=symbol,
                usd_volume=usd_count,
                price=price,
            )
            if size is None:
                return None

            size = str(size)

            body = {
                "symbol": symbol,
                "productType": 'USDT-FUTURES',
                "marginCoin": "USDT",
                "marginMode": 'crossed',
                "size": size,
                "side": side.lower(),
                "orderType": "market",
            }

            body_str = json.dumps(body, separators=(",", ":"))
            
            headers = self._headers(
                method=method,
                path=request_path,
                body=body_str,
            )
            
            async with self.session.post(
                self.url_order,
                headers=headers,
                data=body_str
            ) as response:
                response = await response.json(content_type=None)
                if response.get('code') == '00000':
                    self.logger.success(
                        f'[BITGET ORDER]\n{side} {symbol} - ${volume}'
                        )
                return response
        except Exception as error:
            self.logger.error(
                f'[BITGET ERROR]\n'
                f'{symbol}\nError - {error}'
                )
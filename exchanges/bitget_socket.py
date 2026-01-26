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
from models import SymbolExchange, Order_rules, Base_exchange
from pprint import pprint

class Bitget_order_rules(Order_rules):
    size_multiplier: Decimal | None = None
    volume_place: Decimal | None = None
    min_trade: Decimal | None = None
    min_usdt: Decimal | None = None
    max_qty: Decimal | None = None

class Bitget(Base_exchange):
    def __init__(self, to_manager_queue):
        super().__init__(to_manager_queue)
        self.exchange_name = 'bitget'
        self.to_manager_queue = to_manager_queue
        self.API_KEY = os.getenv('BITGET_API_KEY')
        self.SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
        self.PASSWORD = os.getenv('BITGET_PASSWORD')
        self.socket_url = 'wss://ws.bitget.com/v2/ws/public'
        self.base_url = 'https://api.bitget.com'

    async def _ping_loop(self, websocket):
        while True:
            try:
                await websocket.send("ping")
                await asyncio.sleep(20)
            except Exception as e:
                self.logger.error(f"[BITGET][PING][ERROR] {e}")
                return

    async def _get_symbols(self):
        while True:
            try:
                async with self.session.get(
                    f'{self.api_base_market}/api/v2/mix/market/contracts?productType=USDT-FUTURES'
                ) as response:
                    raw = await response.json()
                    data = raw['data']

                    for item in data:

                        symbolType = item.get('symbolType') == "perpetual"
                        symbolStatus = item.get('symbolStatus') == "normal"
                        minTradeNum = item.get('minTradeNum') > 0
                        sizeMultiplier = item.get('sizeMultiplier') > 0

                        if (
                            symbolType and
                            symbolStatus and
                            minTradeNum and
                            sizeMultiplier
                        ):

                            symbol = item['symbol']
                            self.symbols[symbol] = SymbolExchange(
                                symbol=symbol,
                                exchange=self.exchange_name,
                                order_rules=Bitget_order_rules(
                                    size_multiplier=Decimal(item['sizeMultiplier']),
                                    volume_place=Decimal(item['volumePlace']),
                                    min_trade=Decimal(item['minTradeNum']),
                                    min_usdt=Decimal(item['minTradeUSDT']),
                                    max_qty=Decimal(item['maxMarketOrderQty']),
                                )
                            )
                break
            except Exception as error:
                self.logger.error(
                    f'[{self.exchange_name}][ERROR]\n{error}'
                )
    
    async def run(self):
        await self._get_symbols()
        asyncio.create_task(self._wait_ready())

        subscribe_settings = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": symbol
                } for symbol in self.symbols.keys()
            ]
        }

        while True:
            try:
                async with websockets.connect(
                        self.url_socket,
                        ping_interval=None,
                        close_timeout=5
                ) as websocket:
                    
                    ping_task = asyncio.create_task(self._ping_loop(websocket))
                    await websocket.send(json.dumps(subscribe_settings))
                    self.connection = True
                    self.logger.debug('[BITGET SYSTEM] Соединение установлено')
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

                            self.symbols[symbol].price = last_price
                            self.symbols[symbol].funding = funding
                            self.symbols[symbol].next_funding = int(next_funding_time) / 1000
                            self.symbols[symbol].last_funding_update = time.time()

                        await self.to_manager_queue.put(self.symbols[symbol])

            except Exception as error:
                self.connection = False
                self.logger.error(f'[BITGET ERROR] Произошла ошибка в сокете - {error}. Попытка реконнекта через 2 секунды')
                await asyncio.sleep(2)
            finally:
                if 'ping_task' in locals():
                    ping_task.cancel()

    def _timestamp(
            self
    ):
        return str(int(time.time() * 1000))
    
    def _convert_usd_contracts(self, usd_amount, price, symbol):
        
        usd_volume = Decimal(usd_amount)
        price = Decimal(price)

        if price == 0:
            self.logger.warning(
                f'[BITGET][ORDER][WARNING]\nпрайс равен нулю'
            )
            return None

        order_rules: Bitget_order_rules = self.symbols[symbol].order_rules

        base_qty = usd_volume / price

        size_multiplier = order_rules.size_multiplier
        base_qty = (base_qty / size_multiplier).to_integral_value(rounding=ROUND_DOWN) * size_multiplier

        volume_place = order_rules.volume_place
        quant = Decimal('1').scaleb(-volume_place)

        base_qty = base_qty.quantize(quant, rounding=ROUND_DOWN)

        min_trade = order_rules.min_trade
        min_usdt = order_rules.min_usdt
        max_qty = order_rules.max_qty

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

            size = self._convert_usd_contracts(
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
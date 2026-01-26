import asyncio
import os
import aiohttp
import websockets
import json
from decimal import Decimal, ROUND_DOWN
import time
import hmac
import hashlib
from dotenv import load_dotenv
from models import SymbolExchange, Order_rules, Base_exchange
from pprint import pprint

load_dotenv()

class Binance_order_rules(Order_rules):
    lot_size: Decimal | None = None
    stepSize: Decimal | None = None
    minQty: Decimal | None = None
    min_notional: Decimal | None = None


class Binance(Base_exchange):
    name = 'binance'

    def __init__(self, to_manager_queue):
        super().__init__(to_manager_queue)
        self.API_KEY = os.getenv('BINANCE_API_KEY')
        self.SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')
        self.socket_url = 'wss://stream.binance.com:9443/stream?streams='
        self.base_url = 'https://fapi.binance.com'

    async def _ping_loop(self):
        """
        TODO: ping loop
        """
        pass

    async def _get_symbols(self):
        try:
            sub_list = []
            async with self.session.get(self.base_url + '/fapi/v1/premiumIndex') as response:
                raw = await response.json()
                for item in raw:
                    symbol = item.get('symbol')
                    self.symbols[symbol] = SymbolExchange(
                        symbol=symbol,
                        exchange=self.exchange_name,
                        funding=Decimal(float(item.get('lastFundingRate', 0))) * 100,
                        next_funding=int(item.get('nextFundingTime', 0)) / 1000,
                        last_funding_update=int(item.get('time', 0)) / 1000,
                        order_rules=None
                    )
                    sub_list.append(symbol)
            streams = '/'.join([s.lower() + '@ticker' for s in sub_list])
            self.socket_url = f"{self.socket_url}:9443/stream?streams={streams}"
        except Exception as error:
            self.logger.error(f'[BINANCE][SOCKET][ERROR]\n{error}')
        
        try:
            async with self.session.get(self.base_url + '/fapi/v1/exchangeInfo') as response:
                raw = await response.json()

                for symbol_item in raw['symbols']:
                    status = symbol_item['status'] == 'TRADING'
                    contractType = symbol_item['contractType'] == 'PERPETUAL'
                    if status and contractType:

                        filters = symbol_item['filters']

                        lot_size = next((f for f in filters if f["filterType"] == "MARKET_LOT_SIZE"), None)
                        if not lot_size:
                            lot_size = next(f for f in filters if f["filterType"] == "LOT_SIZE")

                        notional_filter = next(f for f in filters if f["filterType"] == "MIN_NOTIONAL")

                        order_rules = Binance_order_rules(
                            symbol=symbol_item['symbol'],
                            exchange='binance',
                            stepSize=Decimal(lot_size['stepSize']),
                            minQty=Decimal(lot_size['minQty']),
                            min_notional=Decimal(notional_filter['notional'])
                        )
                        if symbol in self.symbols:
                            self.symbols[symbol].order_rules = order_rules
                        else:
                            self.logger.warning('[BINANCE][WARNING]\nнет символа, но есть правила ордера')

        except Exception as error:
            self.logger.error(f'[BINANCE][SOCKET][ERROR]\n{error}')

    async def run(self):
        await self._get_symbols()
        asyncio.create_task(self._wait_ready())
        while True:
            try:
                async with websockets.connect(self.socket_url) as websocket:
                    self.connection = True
                    self.logger.debug('[BINANCE][SOCKET]\nСоединение уставнолено')
                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        data = message['data']
                        symbol = data['s']
                        price = Decimal(float(data['c']))
                        self.symbols[symbol].price = price
                        await self.to_manager_queue.put([self.symbols[symbol]])

            except Exception as error:
                self.connection = False
                self.logger.error(f'[BINANCE][SOCKET][ERROR]\nОшибка в сокете: {error}. Переподключаюсь через 5 секунд...')
                await asyncio.sleep(5)
    
    async def get_funding(self, symbol: str) -> None:
        """
        Docstring для Binance.get_funding
        
        :param symbol: Символ, которому необходимо обновить фандинг
        :type symbol: str
        """
        try:
            async with self.session.get(
                self.base_url + '/fapi/v1/premiumIndex'
                ) as response:
                data = await response.json()
                for item in data:
                    symbol = item.get('symbol')
                    if symbol in self.symbols.keys():
                        funding = Decimal(float(item.get('lastFundingRate', 0))) * 100
                        next_funding_time = Decimal(int(item.get('nextFundingTime', 0)))
                        self.symbols[symbol].funding = funding
                        self.symbols[symbol].next_funding = next_funding_time
                        self.symbols[symbol].last_funding_update = time.time()
        except Exception as error:
            self.logger.error(f'[BINANCE][FUNDING][ERROR]\nОшибка при запросе фандингов с API\nОшибка - {error}')

    async def _convert_usd_contracts(
            self,
            usd_amount: Decimal,
            price: Decimal,
            symbol: str,
            ) -> Decimal:
        """
        Конвертирует сумму в USDT в количество контрактов для Binance Futures.
        usd_amount – сумма в долларах
        price – текущая цена инструмента
        """

        price = Decimal(price)
        usd_amount = Decimal(usd_amount)
    
        stepSize = self.symbols[symbol].order_rules.stepSize
        minQty = self.symbols[symbol].order_rules.minQty
        min_notional = self.symbols[symbol].order_rules.min_notional

        if usd_amount < min_notional:
            self.logger.warning(
                f"[BINANCE SYSTEM] Объём {usd_amount} USD слишком мал "
                f"для {symbol}, минималка {min_notional}"
            )
            return None
    
        qty = usd_amount / price
    
        step = stepSize.normalize()
        qty = (qty // step) * step
    
        if qty < minQty:
            self.logger.warning(
                f"[BINANCE SYSTEM] Объём {qty} контрактов слишком мал для {symbol}, минималка {minQty}"
            )
            return None
    
        return qty.quantize(stepSize, rounding=ROUND_DOWN)


    async def place_order(
        self,
        symbol: str,
        side: str,
        volume: Decimal,
        price: Decimal,
        **kwargs
    ):
        """
        Размещает маркет-ордер на Binance Futures.
        symbol – инструмент, например 'BTCUSDT'
        side – 'BUY' или 'SELL'
        usd_count – сумма в USDT
        price – текущая цена (для конвертации в контракты)
        """

        usd_count = volume

        quantity_size = await self._convert_usd_contracts(
            usd_amount=usd_count,
            price=price,
            symbol=symbol,
        )
        if not quantity_size:
            return None

        qty_str = format(quantity_size, 'f')

        timestamp = int(time.time() * 1000)
        query = (
            f"symbol={symbol}&side={side}&type=MARKET"
            f"&quantity={qty_str}&timestamp={timestamp}"
        )
        signature = self._make_signature(
            query_str=query,
            secret_key=self.SECRET_KEY
            )

        url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.API_KEY}

        try:
            async with self.session.post(url, headers=headers) as resp:
                response = await resp.json()
                if response.get('status') == "NEW":
                    self.logger.success(
                        f'[BINANCE ORDER]\n{side} {symbol} - ${volume}'
                        )
                return response
        except Exception as e:
            self.logger.error(f"[BINANCE ORDER] Запрос сдох: {e}")
            return None

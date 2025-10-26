import asyncio
import os
import aiohttp
import websockets
import requests
import json
from decimal import Decimal, ROUND_DOWN
import time
import hmac
import hashlib
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

class WS_binance:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.symbols = []
        self.symbols_order_info = {}
        self.data = {'stock': 'binance'}
        self.ready = False
        self.url_4_prices = None
        self.url_4_symbol = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        self.connection = False
        self.url_4_fundings = 'https://fapi.binance.com/fapi/v1/premiumIndex'
        self.ready_event = asyncio.Event()
        self.session = None
        self.API_KEY = os.getenv('BINANCE_API_KEY')
        self.SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

    def load_symbols(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(dir_path, 'binance_symbols.json')
        with open(path, 'r') as file:
            self.symbols = json.load(file)
        self.url_4_prices = f"wss://stream.binance.com:9443/stream?streams={'/'.join([s.lower() + '@ticker' for s in self.symbols])}"

    async def start_socket(self):
        self.load_symbols()
        self.session = aiohttp.ClientSession()
        while True:
            try:
                async with websockets.connect(self.url_4_prices) as websocket:
                    self.connection = True
                    self.logger.debug('[BINANCE SYSTEM] Соединение уставнолено')
                    while True:
                        msg = await websocket.recv()
                        message = json.loads(msg)
                        data = message['data']
                        self.data[data['s']] = {
                            'price': float(data['c']),
                            'funding': None,
                            'next_funding_time': None,
                            'time': int(data['E'])
                        }

                        if not self.ready and len(self.data) > 30:
                            self.logger.success('[BINANCE SYSTEM] Данных достаточно. Биржа готова.')
                            self.ready = True
                            self.ready_event.set()
                        
                        await self.cache_manager.set_price(data['s'], 'binance', float(data['c']))

            except Exception as error:
                self.connection = False
                self.logger.error(f'[BINANCE SYSTEM] Ошибка в сокете: {error}. Переподключаюсь через 5 секунд...')
                await asyncio.sleep(5)

    def check_connection(self):
        if self.connection:
            return True
        else:
            return None

    def get_prices_data(self):
        return self.data

    def _check_WTF(self, msg):
        pass

    async def get_funding_4_cur_symbols(self, symbols_list) -> dict:
        # self.logger.debug('[BINANCE SYSTEM] Прямой сбор фандингов с API без кэша')
        funding_dict = {}

        try:
            async with self.session.get(self.url_4_fundings) as response:
                data = await response.json()
                funding_dict = {}
                for item in data:
                    symbol = item.get('symbol')
                    if symbol in symbols_list:
                        funding = float(item.get('lastFundingRate', 0)) * 100
                        next_funding_time = int(item.get('nextFundingTime', 0))
                        funding_dict[symbol] = {
                            'funding': funding,
                            'next_funding_time': next_funding_time,
                        }
                return funding_dict

        except Exception as error:
            self.logger.error(f'[BINANCE ERROR] Ошибка при запросе фандингов с API\nОшибка - {error}')

    async def _usd_to_contracts(
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

        # грузим инфу по символу
        if symbol not in self.symbols_order_info:
            async with self.session.get(self.url_4_symbol) as response:
                raw = await response.json()

                # ищем нужный символ в списке
                symbol_data = next((s for s in raw["symbols"] if s["symbol"] == symbol), None)
                if not symbol_data:
                    self.logger.error(f"[BINANCE ORDER] Нет данных по символу {symbol}")
                    return None

                # кэшируем
                self.symbols_order_info[symbol] = symbol_data
        else:
            symbol_data = self.symbols_order_info[symbol]
    
        filters = symbol_data["filters"]
    
        # для маркетов приоритетнее MARKET_LOT_SIZE
        lot_size = next((f for f in filters if f["filterType"] == "MARKET_LOT_SIZE"), None)
        if lot_size is None:
            lot_size = next(f for f in filters if f["filterType"] == "LOT_SIZE")
    
        notional_filter = next(f for f in filters if f["filterType"] == "MIN_NOTIONAL")
    
        stepSize = Decimal(lot_size["stepSize"])
        minQty = Decimal(lot_size["minQty"])
        min_notional = Decimal(notional_filter["notional"])
    
        if usd_amount < min_notional:
            self.logger.warning(
                f"[BINANCE SYSTEM] Объём {usd_amount} USD слишком мал "
                f"для {symbol}, минималка {min_notional}"
            )
            return None
    
        qty = usd_amount / price
    
        # округляем вниз до кратности stepSize
        step = stepSize.normalize()
        qty = (qty // step) * step
    
        if qty < minQty:
            self.logger.warning(
                f"[BINANCE SYSTEM] Объём {qty} контрактов слишком мал для {symbol}, минималка {minQty}"
            )
            return None
    
        return qty.quantize(stepSize, rounding=ROUND_DOWN)
    
    def _make_signature(
            self,
            query_str: str,
            secret_key: str,
    ):
        return hmac.new(
            secret_key.encode('utf-8'),
            query_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()


    async def place_order(
        self,
        symbol: str,
        side: str,
        volume: Decimal,
        price: Decimal,
        **kwargs
    ):
        """
        ПЕРЕПИСАТЬ НАХУЙ ВСЁ


        Размещает маркет-ордер на Binance Futures.
        symbol – инструмент, например 'BTCUSDT'
        side – 'BUY' или 'SELL'
        usd_count – сумма в USDT
        price – текущая цена (для конвертации в контракты)
        """

        usd_count = volume

        # считаем размер в контрактах
        quantity_size = await self._usd_to_contracts(
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
                return response
        except Exception as e:
            self.logger.error(f"[BINANCE ORDER] Запрос сдох: {e}")
            return None








def get_coins_with_status_TRADING() -> list:
    url_4_symbol = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    response = requests.get(url_4_symbol)
    data = response.json()
    coins_list = []

    for item in data['symbols']:
        if (item.get('status') == 'TRADING'
            and item.get('contractType') == 'PERPETUAL'
            and item.get('quoteAsset') == 'USDT'
        ):
            coins_list.append(item.get('symbol'))

    with open('binance_symbols.json', 'w') as file:
        json.dump(coins_list, file, indent=4, ensure_ascii=False)

get_coins_with_status_TRADING()
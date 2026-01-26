import asyncio
import websockets
import json
import aiohttp
import time as pytime
from decimal import Decimal

from models import SymbolExchange
from typing import Literal

class WS_gate:
    """
    TODO: 
    - убедиться в работе сокета
    - order_func()
    """

    def __init__(
            self,
            logger,
            cache_manager,
            ):
        self.echange_name = 'gate'
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()

        self.symbols_data: dict[str, SymbolExchange]

        self.ready = False
        self.url_4_price = 'wss://fx-ws.gateio.ws/v4/ws/usdt'
        self.url_4_all = 'https://api.gate.io/api/v4/futures/usdt/contracts'
        self.connection = False
        self.session = None

    async def _get_all_symbols(self):
        try:
            host = "https://api.gateio.ws"
            prefix = "/api/v4"
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
                }

            url = '/futures/usdt/contracts'
            url = host + prefix + url

            async with self.session.get(
                url=self.url_4_all,
                headers=headers,
                ) as response:
                raw = await response.json()
                for r in raw:
                    if (
                        r.get('status') == 'trading' and
                        r.get('in_delisting') == False and
                        (
                            int(pytime.time()) < int(r.get('delisting_time')) and
                            r.get('delisting_time') != 0
                        )
                    ):
                        symbol = r.get('name')
                        symbol = "".join(symbol.split("_"))
                        last_price = Decimal(r.get('last_price'))
                        funding = Decimal(r.get('funding_rate'))
                        next_time = Decimal(r.get('funding_next_apply')) - Decimal(pytime.time())
                        # и данные для ордера
                        self._update_symbol(
                            symbol=symbol,
                            price=last_price,
                            funding=funding,
                            next_time=next_time,
                            state='live',
                        )
        except Exception as error:
            self.logger.error(f'[GATE][SOCKET][ERROR] ошибка сбора символов,\n{error}')


    def _update_symbol(
            self,
            symbol: str,
            price: Decimal | None,
            funding: Decimal | None,
            next_time: float | None,
            state: Literal['live', 'dead']
            ):
        """
        Docstring для _update_symbol
        
        :param symbol: Символ
        :param price: Цена
        :param funding: Фандинг
        :param next_time: Время до следующего фандинга
        """
        if symbol in self.symbols_data:
            symbol_data = self.symbols_data[symbol] 
            if price:
                symbol_data.price = price
            if funding:
                symbol_data.funding = funding
            if next_time:
                symbol_data.next_funding = next_time

        elif symbol not in self.symbols_data:
            if state == 'live':
                self.symbols_data[symbol] = SymbolExchange(
                    symbol=symbol,
                    exchange=self.echange_name,
                    price=price,
                    funding=funding,
                    next_funding=next_time,
                )


    async def start_socket(self):
        self.session = aiohttp.ClientSession()
        while True:
            try:
                async with websockets.connect(self.url_4_price) as websocket:
                    self.connection = True
                    self.logger.debug('[GATE][SOCKET] Соединение установлено')

                    subscribe_settings = {
                        "time": int(pytime.time()),
                        "channel": "futures.tickers",
                        "event": "subscribe",
                        "payload": "!all"
                    }
                    await websocket.send(json.dumps(subscribe_settings))
                    self.logger.debug('[GATE][SOCKET] Подписка отправлена')

                    while True:
                        try:
                            msg = await websocket.recv()
                            message = json.loads(msg)

                            event = message.get('event')
                            data = message.get('result')

                            match event:
                                case 'update':
                                    """

        {
  "time": 1541659086,
  "time_ms": 1541659086123,
  "channel": "futures.tickers",
  "event": "update",
  "result": [
    {
      "contract": "BTC_USDT",
      "last": "118.4",
      "change_percentage": "0.77",
      "funding_rate": "-0.000114",
      "funding_rate_indicative": "0.01875",
      "mark_price": "118.35",
      "index_price": "118.36",
      "total_size": "73648",
      "volume_24h": "745487577",
      "volume_24h_btc": "117",
      "volume_24h_usd": "419950",
      "quanto_base_rate": "",
      "volume_24h_quote": "1665006",
      "volume_24h_settle": "178",
      "volume_24h_base": "5526",
      "low_24h": "99.2",
      "high_24h": "132.5"
    }
  ]
}
                                    """
                                    for d in data:
                                        symbol = d.get('contract')
                                        symbol = "".join(symbol.split("_"))
                                        price = d.get('last')
                                        funding = d.get('funding_rate')
                                        self._update_symbol(
                                                symbol=symbol,
                                                price=price,
                                                funding=funding,
                                        )
                                case 'subscribe':
                                    """
{
  "time": 1545404023,
  "time_ms": 1545404023123,
  "channel": "futures.tickers",
  "event": "subscribe",
  "result": {
    "status": "success"
  }
}
                                    """
                                    if data.get('status') == 'success':
                                        self.logger.debug('[GATE][SOCKET] Подписка отправлена')
                                case 'unsubscribe':
                                    """
{
  "time": 1545404900,
  "time_ms": 1545404900123,
  "channel": "futures.tickers",
  "event": "unsubscribe",
  "result": {
    "status": "success"
  }
}
                                    """
                                    if data.get('status') == 'success':
                                        self.logger.debug('[GATE][SOCKET] Подписка отозвана')

                            await self.cache_manager.set_price(symbol, 'gate', price)

                            if not self.ready and len(self.symbols_data) > 30:
                                self.logger.success('[GATE][SOCKET] Биржа готова.')
                                self.ready = True
                                self.ready_event.set()
                        except Exception as error:
                            self.logger.warning(f'[GATE][SOCKET] Ошибка обработки сообщения: {error}')
            except Exception as error:
                self.logger.error(f'[GATE][SOCKET][ERROR] Сокет сдох - {error}. Реконнект через 5 секунд')
                await asyncio.sleep(5)

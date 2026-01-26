import websockets
import asyncio
import aiohttp
import json
import base64
import hmac
import hashlib
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import logging
from pprint import pprint
from decimal import Decimal, ROUND_DOWN
from models import SymbolExchange, Base_exchange, Order_rules

load_dotenv()

class Okx_order_rules(Order_rules):
    ctVal: Decimal | None = None
    lotSz: Decimal | None = None
    minSz: Decimal | None = None

class Okx(Base_exchange):
    def __init__(self, exch_name, to_manager_queue):
        super().__init__(exch_name, to_manager_queue)
        self.exchange_name = exch_name
        self.to_manager_queue = to_manager_queue
        self.API_KEY = os.getenv('OKX_API_KEY')
        self.SECRET_KEY = os.getenv('OKX_SECRET_KEY')
        self.PASSPHRASE = os.getenv('OKX_PASSPHRASE')
        self.base_url = 'https://www.okx.com'
        self.socket_url = 'wss://ws.okx.com:8443/ws/v5/public'

    def replace_to_instID(self, symbol):
        pass
    def replace_to_symbol(self, instID):
        pass

    async def _get_symbols(self):
        async with self.session.get(self.base_url + '/api/v5/public/instruments?instType=SWAP') as response:
            raw = await response.json()
            data: list = raw['data']

            for item in data:
                if (item.get('instType') == 'SWAP' and
                    item.get('state') == 'live'):
                        
                        instId = item.get('instId')
                        symbol = item.get('uly').replace('-', '')
                        ctVal = item.get('ctVal')
                        lotSz = item.get('lotSz')
                        minSz = item.get('minSz')

                        self.symbols[symbol] = SymbolExchange(
                            symbol=symbol,
                            exchange=self.exchange_name,
                            order_rules=Okx_order_rules(
                                symbol=symbol,
                                exchange=self.exchange_name,
                                ctVal=ctVal,
                                lotSz=lotSz,
                                minSz=minSz,
                            ),
                        )

    async def _ping_loop(self, websocket):
        while True:
            try:
                await websocket.send(json.dumps({"op": "ping"}))
                # self.logger.debug("[OKX SOCKET] Ping → серверу")
            except Exception as e:
                self.logger.error(f"[OKX SOCKET] Пинг сдох: {e}")
                break
            await asyncio.sleep(20)  # OKX рекомендует <=30 сек

    async def run(self):
        await self._get_symbols()

        while True:
            try:
                async with websockets.connect(
                    self.socket_url,
                    ping_interval=None
                    ) as websocket:

                    asyncio.create_task(self._ping_loop(websocket))

                    # Подписка на чанки по 30 инструментов
                    for i in range(0, len(self.instId_list), 30):
                        chunk = self.instId_list[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [{"channel": "tickers", "instId": instid} for instid in chunk]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)
                    self.connection = True
                    self.logger.debug('[OKX SOCKET] Соединение установлено')

                    while True:
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=15)
                        except asyncio.TimeoutError:
                            self.logger.warning('[OKX] recv таймаут — повтор чтения')
                            continue

                        try:
                            message = json.loads(msg)
                        except Exception as e:
                            self.logger.warning(f'[OKX] JSON ошибка: {e} — raw: {msg}')
                            continue

                        if 'event' in message or 'data' not in message:
                            continue

                        data = message['data'][0]
                        instId = data['instId']
                        symbol = self.instId_map.get(instId)

                        if not symbol:
                            continue

                        price = data.get('last')
                        ts = data.get('ts')

                        if price in ('', None):
                            continue

                        self.data[symbol] = self.data.get(symbol, {})
                        self.data[symbol]['price'] = float(price)
                        self.data[symbol]['time'] = int(ts)
                        self.data[symbol]['funding'] = None
                        self.data[symbol]['next_funding_time'] = None

                        await self.cache_manager.set_price(symbol, 'okx', price)

                        if not self.ready and len(self.data) > 30:
                            self.logger.success('[OKX SYSTEM] Биржа готова.')
                            self.ready = True
                            self.ready_event.set()

            except Exception as error:
                self.logger.error(f'[OKX ERROR] Сокет сдох - {error}. Реконнект через 2 секунды')
                await asyncio.sleep(2)
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


load_dotenv()

class WS_okx:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.ready_event = asyncio.Event()
        self.symbols = []
        self.instId_list = []
        self.data = {'stock': 'okx'}
        self.ready = False
        self.url_4_symbols = 'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
        self.url_4_prices = 'wss://ws.okx.com:8443/ws/v5/public'
        self.url_for_order = "https://www.okx.com/api/v5/trade/order"
        self.url_4_ctVal = 'https://www.okx.com/api/v5/public/instruments?instId='
        self.connection = False
        self.instId_map = {}
        self.API_KEY = os.getenv('OKX_API_KEY')
        self.SECRET_KEY = os.getenv('OKX_SECRET_KEY')
        self.PASSPHRASE = os.getenv('OKX_PASSPHRASE')
        self.session = None
    
    async def ping_task(self, websocket):
        while True:
            try:
                await websocket.send(json.dumps({"op": "ping"}))
                # self.logger.debug("[OKX SOCKET] Ping → серверу")
            except Exception as e:
                self.logger.error(f"[OKX SOCKET] Пинг сдох: {e}")
                break
            await asyncio.sleep(20)  # OKX рекомендует <=30 сек

    async def start_socket(self, queue):

        self.session = aiohttp.ClientSession()

        await self.get_symbols()


        while True:
            try:
                async with websockets.connect(self.url_4_prices, ping_interval=None) as websocket:

                    asyncio.create_task(self.ping_task(websocket))

                    self.connection = True

                    # Подписка на чанки по 30 инструментов
                    for i in range(0, len(self.instId_list), 30):
                        chunk = self.instId_list[i:i + 30]
                        subscribe_settings = {
                            "op": "subscribe",
                            "args": [{"channel": "tickers", "instId": instid} for instid in chunk]
                        }
                        await websocket.send(json.dumps(subscribe_settings))
                        await asyncio.sleep(0.1)

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


    async def get_symbols(self):
        self.instId_list = []
        self.symbols = []
        while True:
            try:
                async with self.session.get(self.url_4_symbols) as response:
                    data = await response.json()
                    for item in data.get('data', []):
                        if item['settleCcy'] == 'USDT' and item['state'] == 'live' and item['ctType'] == 'linear':
                            instId = item.get('instId')
                            symbol = item.get('uly').replace('-', '')
                            self.instId_map[instId] = symbol
                            self.instId_list.append(instId)
                            self.symbols.append(symbol)
                            ctVal = item.get('ctVal')
                            lotSz = item.get('lotSz')
                            minSz = item.get('minSz')

                            self.data[symbol] = {
                                'instId': instId,
                                'price': None,
                                'funding': None,
                                'next_funding_time': None,
                                'time': None,
                                'ctVal': ctVal,
                                'lotSz': lotSz,
                                'minSz': minSz,
                            }
                    return
            except Exception as error:
                self.logger.error(f'[OKX ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)


    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        try:
            instId = f"{symbol.replace('USDT', '')}-USDT-SWAP"
            url = f'https://www.okx.com/api/v5/public/funding-rate?instId={instId}'

            async with self.session.get(url) as response:
                data = await response.json()
                if data.get('code') != '0' or not data.get('data'):
                    return {}

                record = data['data'][0]
                funding_raw = record.get('fundingRate')
                next_time_raw = record.get('nextFundingTime')

                if not funding_raw or not next_time_raw:
                    self.logger.warning(f"[OKX WARNING] У {symbol} funding или nextFundingTime пустые → скипаем")
                    return {}

                funding = float(funding_raw) * 100
                next_time = int(next_time_raw)

                return {
                    symbol: {
                        'funding': funding,
                        'next_funding_time': next_time,
                    }
                }

        except Exception as error:
            self.logger.error(f'[OKX ERROR] REST фандинг по {symbol} сдох: {error}')
            return {}


    def get_prices_data(self):
        return self.data
    
    async def _usd_to_contracts(
        self,
        instId: str,
        volume: Decimal,
        price:Decimal,
        ):

        usd_count = volume

        try:
            symbol = self.instId_map[instId]
            ctVal = Decimal(str(self.data[symbol]['ctVal'])) #Decimal(data[0]['ctVal'])
            lotSz = Decimal(str(self.data[symbol]['lotSz'])) #Decimal(data[0]['lotSz'])
            minSz = Decimal(str(self.data[symbol]['minSz']))
            sz = Decimal(str(usd_count)) / (price * ctVal)
            size = (sz // lotSz) * lotSz
            if size < minSz:
                self.logger.warning(f"[OKX SYSTEM] Объём {usd_count} USD слишком мал для {instId}, минималка {minSz}")
                return None
            
            return format(size, 'f')
        except Exception as error:
            self.logger.error(f'[OKX SYSTEM] Произошел сбой при конвертации бабла в контракты\nОшибка - {error}')
            return None
    
    def make_signature(self, timestamp, body: str):
        try:
            message = timestamp + 'POST' + '/api/v5/trade/order' + body
            signature = base64.b64encode(
                hmac.new(self.SECRET_KEY.encode('utf-8'),
                         message.encode('utf-8'),
                         hashlib.sha256).digest()
            ).decode()
            return signature
        except Exception as error:
            self.logger.error(f'[OKX SYSTEM] Произошел сбой при создании подписи для ордера\nОшибка - {error}')
            return None
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        posSide: str,
        volume: Decimal,
        price: Decimal,
        ):
        instId = symbol.removesuffix('USDT') + '-USDT-SWAP'

        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

        usd_count = volume

        size = await self._usd_to_contracts(
            instId=instId,
            volume=usd_count,
            price=price,
        )
        if size is None:
            return None
        print('OKX POSSDIE')
        print(posSide)

        body = json.dumps({
            "instId": instId,
            "tdMode": "cross",
            "side": side.lower(),
            # "posSide": posSide, # long / short
            "ordType": "market",
            "sz": size
        })

        headers = {
            "OK-ACCESS-KEY": self.API_KEY,
            "OK-ACCESS-SIGN": self.make_signature(timestamp=timestamp, body=body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.PASSPHRASE,
            "Content-Type": "application/json",
        }

        async with self.session.post(
            url=self.url_for_order,
            headers=headers,
            data=body,
        ) as response:
            response = await response.json()
            return response





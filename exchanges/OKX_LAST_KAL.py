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


    async def start_socket(self, queue):

        self.session = aiohttp.ClientSession()

        await self.get_symbols()


        while True:
            try:
                async with websockets.connect(self.url_4_prices, ping_interval=25, ping_timeout=10) as websocket:

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

                        if not price or not ts:
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
                            self.data[symbol] = {
                                'price': None,
                                'funding': None,
                                'next_funding_time': None,
                                'time': None
                            }
                    return
            except Exception as error:
                self.logger.error(f'[OKX ERROR] Ошибка при сборе символов - {error}. Повтор через 5 сек')
                await asyncio.sleep(5)


    async def get_funding_4_cur_symbols(self, symbol: str) -> dict:
        """ Получает funding rate и время следующего фандинга по символу """
        try:
            instId = symbol.removesuffix('USDT') + '-USDT-SWAP'
            url = f'https://www.okx.com/api/v5/public/funding-rate?instId={instId}'

            async with self.session.get(url) as response:
                data = await response.json()
                if data.get('code') != '0' or not data.get('data'):
                    return {}
                funding = float(data['data'][0]['fundingRate']) * 100
                next_time = int(data['data'][0]['nextFundingTime'])
                
                funding_dict = {}
                funding_dict[symbol] = {
                    'funding': funding,
                    'next_funding_time': next_time,
                }
                return funding_dict

        except Exception as error:
            self.logger.error(f'[OKX ERROR] REST фандинг по {symbol} сдох: {error}')
            return {}

    def get_prices_data(self):
        return self.data
    

    async def _usd_to_contracts(self, usd_volume, instId):
        try:
            symbol = self.instId_map[instId]
            async with self.session.get(self.url_4_ctVal + instId) as resp:
                info = await resp.json()
                data_list = info.get('data')
                if not data_list:
                    self.logger.error(f'[OKX SYSTEM] Нет данных по инструменту {instId}')
                    return None

                ctVal = Decimal(data_list[0]['ctVal'])
                lotSz = Decimal(data_list[0]['lotSz'])
                print(lotSz)
                last_price = Decimal(str(self.data[symbol]['price']))

                # считаем контракты
                sz = Decimal(str(usd_volume)) / (last_price * ctVal)

                # округляем вниз до ближайшего кратного lotSz
                sz = sz.quantize(lotSz, rounding=ROUND_DOWN)
                print(sz)

                if sz < lotSz:
                    self.logger.warning(f"[OKX SYSTEM] Объём {usd_volume} USD слишком мал для {instId}, минималка {lotSz}")
                    return None

                # возвращаем строкой для API
                return format(sz, 'f')
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

    async def place_order(self, instId: str, side: str, usd_volume: Decimal):
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

        # считаем размер в контрактах (возвращается уже строка)
        size = await self._usd_to_contracts(
            instId=instId,
            usd_volume=usd_volume
        )

        body_dict = {
            "instId": instId,
            "tdMode": "cross",      # для фьючей или свопов (если надо cash — меняй тут)
            "side": side,
            "ordType": "market",
            "sz": size              # строка, например "0.001"
        }

        body = json.dumps(body_dict, separators=(",", ":"))  # без лишних пробелов

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
            data=body
        ) as resp:
            return await resp.json()



async def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("TEST_OKX")

    # Заглушка кэша
    class DummyCache:
        async def set_price(self, *args, **kwargs):
            pass

    cache = DummyCache()

    # создаём экземпляр WS_okx
    okx = WS_okx(logger, cache)

    # подтягиваем символы
    await okx.get_symbols()

    pprint(okx.instId_map)

    # Выбираем инструмент, который реально есть
    instId = "A-USDT-SWAP"
    symbol = okx.instId_map[instId]

    # Ставим цену вручную, чтобы расчёт контракта прошёл
    okx.data[symbol]['price'] = 25000.0  # или актуальная цена

    # Отправляем ордер один раз
    resp = await okx.place_order(instId=instId, side="buy", usd_volume=10)
    print("Ответ от OKX:", resp)

    await okx.session.close()

if __name__ == "__main__":
    asyncio.run(main())


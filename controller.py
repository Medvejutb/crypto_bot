import asyncio
from exchanges.binance_socket import WS_binance
from exchanges.bitget_socket import WS_bitget
from exchanges.okx import WS_okx
from exchanges.bybit_socket import WS_bybit
from exchanges.gate_socket import WS_gate
from itertools import combinations
from utils.math_operations import Calculator
import config

class Controller:
    def __init__(self):
        self.binance = WS_binance()
        self.bitget = WS_bitget()
        self.okx = WS_okx()
        self.bybit = WS_bybit()
        self.gate = WS_gate()
        self.calculator = Calculator()
        self.stocks_dict = {}
        self.spreads = {}
        self.stocks_fundings_funcs = {
            'binance': self.binance.get_funding_4_cur_symbols,
            'bitget': self.bitget.get_funding_4_cur_symbols,
            'okx': self.okx.get_funding_4_cur_symbols,
            'bybit': self.bybit.get_funding_4_cur_symbols,
            'gate': None
        }


    async def start_all_sockets(self):
        print('[SYSTEM] Запускаем WebSocket Binance...')
        binance_task = asyncio.create_task(self.binance.start_socket())

        print('[SYSTEM] Запускаем WebSocket Bitget...')
        bitget_task = asyncio.create_task(self.bitget.start_socket())

        print('[SYSTEM] Запускаем WebSocket OKX...')
        okx_task = asyncio.create_task(self.okx.start_socket())

        print('[SYSTEM] Запускаем WebSocket BYBIT...')
        bybit_task = asyncio.create_task(self.bybit.start_socket())

        print('[SYSTEM] Запускаем WebSocket GATE...')
        gate_task = asyncio.create_task(self.gate.start_socket())


        ready = {
            'binance': False,
            'bitget': False,
            'okx': False,
            'bybit': False,
            'gate': False
        }

        while not all(ready.values()):
            if not ready['binance'] and self.binance.check_ready():
                print('[SYSTEM] Binance готов. Работаем.')
                ready['binance'] = True
            elif not ready['binance']:
                print('[SYSTEM] Binance ещё не готов. Ждём...')

            if not ready['bitget'] and self.bitget.check_ready():
                print('[SYSTEM] Bitget готов. Работаем.')
                ready['bitget'] = True
            elif not ready['bitget']:
                print('[SYSTEM] Bitget ещё не готов. Ждём...')

            if not ready['okx'] and self.okx.check_ready():
                print('[SYSTEM] OKX готов. Работаем.')
                ready['okx'] = True
            elif not ready['okx']:
                print('[SYSTEM] OKX ещё не готов. Ждём...')

            if not ready['bybit'] and self.bybit.check_ready():
                print('[SYSTEM] BYBIT готов. Работаем.')
                ready['bybit'] = True
            elif not ready['bybit']:
                print('[SYSTEM] BYBIT ещё не готов. Ждём...')

            if not ready['gate'] and self.gate.check_ready():
                print('[SYSTEM] GATE готов. Работаем.')
                ready['gate'] = True
            elif not ready['gate']:
                print('[SYSTEM] GATE ещё не готов. Ждём...')

            await asyncio.sleep(3)

    def _merge_exchange_data(self, data_list):
        self.stocks_dict = {}

        for stock in data_list:
            stock_name = stock['stock']
            for symbol, data in stock.items():
                if symbol == 'stock':
                    continue
                if symbol not in self.stocks_dict:
                    self.stocks_dict[symbol] = {}

                self.stocks_dict[symbol][stock_name] = {
                    'price': data['price'],
                    'time': data['time']
                }



    def get_needed_symbols_with_prices(self):
        binance_data = self.binance.get_prices_data()
        bitget_data = self.bitget.get_prices_data()
        okx_data = self.okx.get_prices_data()
        bybit_data = self.bybit.get_prices_data()
        gate_data = self.gate.get_prices_data()

        data_list = [
            binance_data,
            bitget_data,
            okx_data,
            bybit_data,
            gate_data
        ]

        self._merge_exchange_data(data_list)

        for symbol, stocks in self.stocks_dict.items():
            stocks_data = list(stocks.keys())
            for stock1, stock2 in combinations(stocks_data, 2):
                price1 = float(stocks[stock1]['price'])
                price2 = float(stocks[stock2]['price'])

                if price2 == 0:
                    continue

                spread = self.calculator.calc_spread(price1, price2)
                self.spreads.setdefault(symbol, {})[f'{stock1}-{stock2}'] = spread

        for symbol, data in self.spreads.items():
            allowed_exchanges = set()

            for stock_pair, spread_value in data.items():
                if abs(spread_value) >= config.SPREAD:
                    exch1, exch2 = stock_pair.split('-')
                    allowed_exchanges.update([exch1, exch2])

            allowed_exchanges = list(allowed_exchanges)[:2]

            if allowed_exchanges:
                self.stocks_dict[symbol] = {
                    exch: info for exch, info in self.stocks_dict[symbol].items()
                    if exch in allowed_exchanges
                }
            else:
                self.stocks_dict[symbol] = {}

        del_symbol_list = []
        for symbol, data in self.stocks_dict.items():
            if not data or len(data) < 2:
                del_symbol_list.append(symbol)

        for symbol in del_symbol_list:
            del self.stocks_dict[symbol]

        return self.stocks_dict

    async def get_fundings_for_symbols(self, symbols_dict):
        fundings_dict = {}

        # Собираем все уникальные symbol'ы, которые есть в symbols_dict
        symbols_set = set(symbols_dict.keys())

        # Для каждой биржи, где есть функция фандинга
        for stock_name, func in self.stocks_fundings_funcs.items():
            if func is None:
                continue

            try:
                print('[SYSTEM] Сбор фандингов')
                stock_funding_data = await func(list(symbols_set))
                # Ожидаем, что вернёт {'BTCUSDT': 0.0001, 'ETHUSDT': 0.0002, ...}

                for symbol, funding in stock_funding_data.items():
                    if symbol not in fundings_dict:
                        fundings_dict[symbol] = {}
                    fundings_dict[symbol][stock_name] = funding

            except Exception as e:
                print(f'[FUNDING ERROR] {stock_name} сдохла: {e}')

        return fundings_dict

    def get_uncorrelations(self, main_dict):
        symbol_dict = {}
        for symbol, stocks in main_dict.items():
            stock_name1, stock_name2 = list(stocks.keys())
            stock1, stock2 = stocks[stock_name1], stocks[stock_name2]

            uncorrelation = self.calculator.calc_uncorrelation(stock1, stock2, stock_name1, stock_name2)


            if uncorrelation:
                symbol_dict[symbol] = uncorrelation

        return symbol_dict

    def check_and_split_msg(self, msg):
        if len(msg) > 10:
            chunks = [msg[i:i + 10] for i in range(0, len(msg), 10)]
            return chunks
        else:
            return [msg]

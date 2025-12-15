import asyncio
from exchanges.binance_socket import WS_binance
from exchanges.bitget_socket import WS_bitget
from exchanges.okx import WS_okx
from exchanges.bybit_socket import WS_bybit
from exchanges.gate_socket import WS_gate
from utils.math_operations import Calculator
from pprint import pprint


class Sockets_manager:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.sockets_ready_event = asyncio.Event()
        self.binance = WS_binance(self.logger, self.cache_manager)
        self.bitget = WS_bitget(self.logger, self.cache_manager)
        self.okx = WS_okx(self.logger, self.cache_manager)
        self.bybit = WS_bybit(self.logger, self.cache_manager)
        self.gate = WS_gate(self.logger, self.cache_manager)
        self.calculator = Calculator()
        self.stocks_dict = {}
        self.spreads = {}
        self._stocks_prices_funcs = {
            'binance': self.binance.get_prices_data,
            'bitget': self.bitget.get_prices_data,
            'okx': self.okx.get_prices_data,
            'bybit': self.bybit.get_prices_data,
            # 'gate':
        }
        self.stocks_fundings_funcs = {
            'binance': self.binance.get_funding_4_cur_symbols,
            'bitget': self.bitget.get_funding_4_cur_symbols,
            'okx': self.okx.get_funding_4_cur_symbols,
            'bybit': self.bybit.get_funding_4_cur_symbols,
            # 'gate': self.gate.get_funding_4_cur_symbols
        }
        self.order_funcs = {
            "binance": self.binance.place_order,
            "bitget": self.bitget.place_order,
            "okx": self.okx.place_order,
            "bybit": None,
            "gate": None,
        }


    async def start_all_sockets(self, queue):
        
        try:
        
            asyncio.create_task(self.binance.start_socket())

            asyncio.create_task(self.bitget.start_socket())

            asyncio.create_task(self.okx.start_socket(queue))

            asyncio.create_task(self.bybit.start_socket(queue))

            # asyncio.create_task(self.gate.start_socket(queue))



            await asyncio.gather(
                self.binance.ready_event.wait(),
                self.bitget.ready_event.wait(),
                self.okx.ready_event.wait(),
                self.bybit.ready_event.wait(),
                # self.gate.ready_event.wait()

            )
            self.sockets_ready_event.set()
            self.logger.success('[SYSTEM] Биржи все биржи готовы')
        
        except Exception as error:
            self.logger.error(f'[SOCKETS ERROR] Ошибка в менеджере сокетов - {error}')
    
    def _collect_stocks_prices_data(self):

        results = {}
        for exchange, func in self._stocks_prices_funcs.items():
            try:
                for symbol, data in func().items():
                    results.setdefault(symbol, {})[exchange] = data
                
            except Exception as error:
                self.logger.error(f'[WEBSOCKETS SYSTEM] Произошла ошибка при объединений данных с бирж\nОшибка - {error}, {exchange}')
                
        return results
    
    def get_prices_from_exchanges(self):
        results = self._collect_stocks_prices_data()
        if results:
            return results
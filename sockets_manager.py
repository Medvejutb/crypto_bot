import asyncio, time, copy

from exchanges.exch_register import EXCHANGE_REGISTRY
from config_file import EXCHANGES as conf_exch_list
from config_file import FUNDING_UPDATE_INTERVAL

from models import Symbol, SymbolExchange, Base_exchange
from pprint import pprint


class Sockets_manager:
    """
    TODO:
    - инициализация объектов символов
    - грамотное создание экземпляров бирж:
        - бинанс и битгет уже переделаны и переименованы
    """
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.sockets_ready_event = asyncio.Event()
        self.from_sockets_queue = asyncio.Queue()
        self.to_uncor_queue = asyncio.Queue()
        self.symbols: dict[str, Symbol] = {}

        self.exchanges: dict[str, Base_exchange] = {}
        
        for exch in conf_exch_list:
            try:
                cls = EXCHANGE_REGISTRY[exch]
                self.exchanges[exch] = cls(self.from_sockets_queue)
            except Exception as e:
                self.logger.warning(f'[EXCHANGES MANAGER][WARNING]\n{e}')
    
    async def get_funding(
            self,
            exch_name,
            symbol,
    ):
        result = await self.exchanges[exch_name].get_funding(symbol)
        return result
    
    async def place_order(
            self,
            exch_name,
            symbol,
            usd,
            price,
            **kwargs,
    ):
        await self.exchanges[exch_name].place_order(
            symbol=symbol,
            price=price,
            volume=usd,
            **kwargs,
        )
    
    async def _wait_ready(self) -> None:
        """
        В фоне ждёт, пока на всех биржах соберется достаточно символов        
        """
        await asyncio.gather(*(exch.ready_event.wait() for exch in self.exchanges.values()))
        self.sockets_ready_event.set()
        self.logger.success("[EXCHANGES MANAGER]\nВсе биржи готовы")

    async def update_fundings(self) -> None:
        try:
            for symbol_obj in self.symbols.values():
                for exch_obj in symbol_obj.exchanges.values():
                    if time.time() >= exch_obj.next_funding:
                        await self.exchanges[exch_obj.exchange].get_funding(symbol_obj.symbol)
                        await asyncio.sleep(0.5)
            self.logger.debug('[EXCHANGES MANAGER][FUNDING]\nФандинги обновлены')
        except Exception as e:
            self.logger.error(f'[EXCHANGES MANAGER][FUNDING][ERROR]\n{e}')

    async def _funding_loop(self) -> None:
        while True:
            await self.update_fundings()
            await asyncio.sleep(FUNDING_UPDATE_INTERVAL)

    async def _start_all_sockets(self) -> None:

        try:
            tasks = []

            for exch_name, exch_obj in self.exchanges.items():
                if hasattr(exch_obj, 'run'):
                    tasks.append(asyncio.create_task(exch_obj.run()))

            if tasks:
                await asyncio.gather(*tasks)
                
        except Exception as error:
            self.logger.error(f'[EXCHANGES MANAGER][ERROR]\nexchange - {exch_name}\n{error}')


    async def run(self):
        """
        Асинхронный воркер, который запускает все биржевые сокеты,
        ждёт их готовности, непрерывно собирает поступающие тики из очереди,
        обновляет состояние всех символов и швыряет атомарный snapshot в очередь для дальнейшей обработки.
        """
        asyncio.create_task(
            self._start_all_sockets()
        )
        await self._wait_ready()
        
        asyncio.create_task(
            self._funding_loop()
        )

        while True:
            try:
                update_batch = []
                while True:
                    try:
                        update = self.from_sockets_queue.get_nowait()
                        update_batch.append(update)
                    except asyncio.QueueEmpty:
                        break
                for update in update_batch:
                    for sym_exch_obj in update:
                        symbol_name = sym_exch_obj.symbol
                        symbol_obj: SymbolExchange = self.symbols.setdefault(
                            symbol_name, Symbol(
                                symbol=symbol_name,
                            )
                        )
                        symbol_obj.exchanges[sym_exch_obj.exchange] = sym_exch_obj
                
                snapshot = copy.deepcopy(self.symbols)
                await self.to_uncor_queue.put(snapshot)

            except Exception as error:
                self.logger.error(f'[EXCHANGES MANAGER][ERROR]\n{error}')
    

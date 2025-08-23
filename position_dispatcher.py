import asyncio
import time
from pprint import pprint

class Position_dispatcher:
    def __init__(self,
                cache_manager,
                logger,
                position_config,
                ):
        self.managers_dict = {}
        self.cache_manager = cache_manager
        self.logger = logger
        self.position_config = position_config
        self.max_count_positions = position_config.MAX_COUNT_POSITIONS
    

    async def start_working(self, uncorrelations_event):
        await uncorrelations_event.wait()
        self.logger.success('[POSITION SYSTEM] Запуск воркера позиций')
        while True:
            try:
                uncorrelations = await self.cache_manager.get_uncor()
            except Exception as error:
                self.logger.error(f'Произошла ошибка при сборе раскорреляций с кэша. Перезапуск через 3 сек\nОшибка - {error}')
                await asyncio.sleep(3)
                continue

            for key in list(self.managers_dict.keys()):
                if self.managers_dict[key].position_state_for_dispatcher == 'cancel':
                    del self.managers_dict[key]
                    await self.cache_manager.del_active_position_symbol_pair(key)
                    print(f'{key} DELETE')

            if uncorrelations:

                # pprint(uncorrelations)
                try:
                    for key, data in uncorrelations.items():
                        uncorrelation = data['difference']
                        higher_exchange = data['higher_exchange']
                        lower_exchange = data['lower_exchange']
                        key = (key, higher_exchange, lower_exchange)

                        if (
                            key not in self.managers_dict and
                            len(self.managers_dict) < self.max_count_positions and
                            uncorrelation >= self.position_config.ENTER_UNCORRELATION_VALUE
                        ):

                            self.managers_dict[key] = Position_manager(
                                uncorrelation_value=uncorrelation,
                                symbol=key,
                                start_time=time.time(),
                                higher_exchange=higher_exchange,
                                lower_exchange=lower_exchange,
                                logger=self.logger,
                                position_config=self.position_config,
                                del_pos_func=lambda k=key: self.del_stillborn_pos(k)
                            )

                            asyncio.create_task(self.managers_dict[key].start_work())
                            await self.managers_dict[key].set_uncorrelation(uncorrelation)

                        if (
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'active' or
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'wait_for_start'
                        ):
                            print('SISKI PISKI POPKI')
                            await self.cache_manager.add_active_position_symbol_pair(key)
                            await self.managers_dict[key].set_uncorrelation(uncorrelation)
                            # print(self.managers_dict[key])
                except Exception as error:
                    self.logger.error(f'[POSITION SYSTEM] Ошибка в диспетчере - {error}')

            await asyncio.sleep(0.5)

    def del_stillborn_pos(self, key):
        if key in self.managers_dict:
            del self.managers_dict[key]

            


class Position_manager:
    
    def __init__(
            self,
            symbol: str,
            start_time,
            uncorrelation_value: float,
            lower_exchange: str,
            higher_exchange: str,
            logger,
            position_config,
            del_pos_func,
            ):
        self.logger = logger

        self.start_time = start_time

        # данные символа
        self.symbol = symbol
        self.lower_exchange = lower_exchange
        self.higher_exchange = higher_exchange

        # параметры раскорреляяций
        self.uncorrelation_value = uncorrelation_value
        self.last_uncorrelation_value = 0.0
        self.MAX_UNCORRELATION_VALUE = position_config.MAX_UNCORRELATION_VALUE
        self.ENTER_UNCORRELATION_VALUE = position_config.ENTER_UNCORRELATION_VALUE
        self.UNCORRELATION_VALUE_RANGE_FOR_EXIT = position_config.UNCORRELATION_VALUE_RANGE_FOR_EXIT
        self.needed_uncor_for_enter_step = self.ENTER_UNCORRELATION_VALUE
        self.needed_uncor_for_exit_step = self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[1]

        self.WAIT_POSITION = position_config.WAIT_POSITION

        self.MONEY_VOLUME = position_config.MONEY_VOLUME
        self.STEP_COUNT = position_config.STEP_COUNT

        self.EXCHANGES_FOR_IN = position_config.EXCHANGES_FOR_IN

        self.del_pos_func = del_pos_func

        self.one_enter_step_in_procent = (self.MAX_UNCORRELATION_VALUE - self.ENTER_UNCORRELATION_VALUE) / self.STEP_COUNT
        self.one_exit_step_in_procent = (self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[1] - self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[0]) / self.STEP_COUNT
        self.WAIT_FOR_STEP = position_config.WAIT_FOR_STEP
        self.enter_steps_done_count = 0
        self.exit_steps_done_count = 0

        self.position_state_for_dispatcher = '' # exit / cancel / active / wait_for_start
        self.position_state_for_manager = '' # cancel / waiting / to_enter_step / to_exit_step

        self.states_handlers = {
            'waiting': self._waiting,
            'to_enter_step': self._to_enter_step,
            'to_exit_step': self._to_exit_step,
            'cancel': self._noop,
        }
    
    async def start_work(self):

        print('CHECK SYMBOL TIME')

        self.position_state_for_dispatcher = 'wait_for_start'

        while time.time() - self.start_time < self.WAIT_POSITION:
            if self.uncorrelation_value < self.ENTER_UNCORRELATION_VALUE:
                self.position_state_for_dispatcher = 'cancel'
                print('Позиция не стартовала')
                return
            print(f'SYMBOL STAYING - {self.uncorrelation_value}')
            self.last_uncorrelation_value = self.uncorrelation_value

            await asyncio.sleep(1)

        self.position_state_for_dispatcher = 'active'
        self.position_state_for_manager = 'to_enter_step'
        self.logger.success(f'[POSITION SYSTEM] {self.symbol} - старт позиции ')

        while True:
            try:
                print(f'\n{self.symbol}\n{self.uncorrelation_value}\n')
                await self.states_handlers[self.position_state_for_manager]()

            except Exception as error:
                self.logger.error(f'[POSITION SYSTEM] {self.symbol} Ошибка в выполнении обработки состояния - {error}')
            await asyncio.sleep(0.5)
    
    async def set_uncorrelation(self, uncorrelation):
        self.uncorrelation_value = uncorrelation

    async def _waiting(self):
        if self.uncorrelation_value >= self.needed_uncor_for_enter_step:
            self.position_state_for_manager = 'to_enter_step'
        if self.uncorrelation_value <= self.needed_uncor_for_exit_step:
            self.position_state_for_manager = 'to_exit_step'
    
    async def _to_enter_step(self):
        self.logger.info(f'Пизданул один шаг входа {self.symbol} - {self.uncorrelation_value}')
        self.needed_uncor_for_enter_step = self.needed_uncor_for_enter_step + self.one_enter_step_in_procent
        self.enter_steps_done_count += 1
        await asyncio.sleep(self.WAIT_FOR_STEP)
        self.position_state_for_manager = 'waiting'

    async def _to_exit_step(self):
        if self.needed_uncor_for_enter_step > self.ENTER_UNCORRELATION_VALUE:
            if self.exit_steps_done_count < self.enter_steps_done_count:
                self.logger.info(f'Хуйнул один шаг выхода {self.symbol} - {self.uncorrelation_value}')
                self.needed_uncor_for_exit_step = self.needed_uncor_for_exit_step - self.one_exit_step_in_procent
                self.exit_steps_done_count += 1
                await asyncio.sleep(self.WAIT_FOR_STEP)
                self.position_state_for_manager = 'waiting'
            else:
                self.position_state_for_dispatcher = 'cancel'
                self.position_state_for_manager = 'cancel'
                self.logger.success(f'[POSITION SYSTEM] {self.symbol} - позиция закрыта ')
                return

    async def _noop(*_):
        return None

    def __str__(self):
        return f'{self.symbol}\nhigher exchange - {self.higher_exchange}\nlower exchange - {self.lower_exchange}\nuncor - {self.uncorrelation_value}'
        
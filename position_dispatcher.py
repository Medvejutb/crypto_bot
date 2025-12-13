import asyncio
from decimal import Decimal
import time
import msgspec
from typing import Literal
from pprint import pprint

class Position_dispatcher:
    def __init__(self,
                cache_manager,
                logger,
                position_config,
                order_manager,
                alert_queue: asyncio.Queue,
                ):
        self.managers_dict = {}
        self.cache_manager = cache_manager
        self.logger = logger
        self.order_manager = order_manager
        self.position_config = position_config
        self.max_count_positions = position_config.MAX_COUNT_POSITIONS
        self.alert_queue = alert_queue
    

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

            # for key in list(self.managers_dict.keys()):
            #     if self.managers_dict[key].position_state_for_dispatcher == 'cancel':
            #         del self.managers_dict[key]
            #         await self.cache_manager.del_active_position_symbol_pair(key)
            #         print(f'{key} DELETE')

            if uncorrelations:

                try:
                    for symbol, data in uncorrelations.items():
                        if symbol == 'COAIUSDT':
                            continue
                        uncorrelation = Decimal(data['difference'])
                        higher_exchange = data['higher_exchange']
                        higher_price = Decimal(data['higher_price'])
                        lower_exchange = data['lower_exchange']
                        lower_price = Decimal(data['lower_price'])
                        key = (symbol, higher_exchange, lower_exchange)

                        if (
                            key not in self.managers_dict and
                            len(self.managers_dict) < self.max_count_positions and
                            uncorrelation >= self.position_config.ENTER_UNCORRELATION_VALUE
                        ):

                            self.managers_dict[key] = Position_manager(
                                order_manager=self.order_manager,
                                uncorrelation_value=uncorrelation,
                                key=key,
                                start_time=time.time(),
                                symbol=symbol,
                                higher_exchange=higher_exchange,
                                higher_price=higher_price,
                                lower_exchange=lower_exchange,
                                lower_price=lower_price,
                                logger=self.logger,
                                position_config=self.position_config,
                                del_pos_func=lambda k=key: self._delete__position(k),
                                alert_queue=self.alert_queue
                            )

                            asyncio.create_task(self.managers_dict[key].start_work())
                            await self.managers_dict[key].set_uncorrelation(uncorrelation)

                        if (
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'active' or
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'wait_for_start'
                        ):
                            await self.cache_manager.add_active_position_symbol_pair(key)
                            await self.managers_dict[key].set_uncorrelation(uncorrelation)
                        
                        elif (
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'cancel' or
                            key in self.managers_dict and self.managers_dict[key].position_state_for_dispatcher == 'close'
                            ):
                            self._delete__position(key=key)


                except Exception as error:
                    self.logger.error(f'[POSITION SYSTEM] Ошибка в диспетчере - {error}')

            await asyncio.sleep(0.5)

    async def _delete__position(self, key):
        if key in self.managers_dict:
            await self.cache_manager.del_active_position_symbol_pair(key)
            del self.managers_dict[key]

class Notification(msgspec.Struct):
    """
    Объект уведомления, который хранит данные о позиций:
    - старт позиций
    - шаг входа
    - шаг выхода
    - закрытие позиций
    - прочие данные для контекста

    TODO: Возможно стоит хранить значения с оригинальным типом данных
    для передачи в alert_manager
    """
    alert_type: Literal['start', 'enter', 'exit', 'close']
    symbol: str
    uncorrelation: str
    total_volume: str
    sell_exch: str
    buy_exch: str
    total_steps: str
    
    part_volume: str | None = None
    step: str | None = None

class Position_manager:
    
    def __init__(
            self,
            key,
            start_time,
            uncorrelation_value: float,
            symbol,
            lower_exchange: str,
            lower_price: str,
            higher_exchange: str,
            higher_price: str,
            logger,
            order_manager,
            position_config,
            del_pos_func,
            alert_queue: asyncio.Queue,
            ):
        self.logger = logger
        self.order_manager = order_manager
        self.alert_queue = alert_queue

        self.start_time = start_time

        # данные символа
        self.pair_key = key # symbol , stock, stock
        self.symbol = symbol
        self.lower_exchange = lower_exchange
        self.lower_price = lower_price
        self.higher_exchange = higher_exchange
        self.higher_price = higher_price

        # параметры раскорреляяций
        self.uncorrelation_value = uncorrelation_value
        self.last_uncorrelation_value = 0.0
        self.MAX_UNCORRELATION_VALUE = position_config.MAX_UNCORRELATION_VALUE
        self.ENTER_UNCORRELATION_VALUE = position_config.ENTER_UNCORRELATION_VALUE
        self.UNCORRELATION_VALUE_RANGE_FOR_EXIT = position_config.UNCORRELATION_VALUE_RANGE_FOR_EXIT
        self.needed_uncor_for_enter_step = self.ENTER_UNCORRELATION_VALUE
        # self.needed_uncor_for_exit_step = self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[1]

        self.WAIT_POSITION = position_config.WAIT_POSITION

        self.MONEY_VOLUME = position_config.MONEY_VOLUME
        self.STEP_COUNT = position_config.STEP_COUNT
        self.money_for_one_step = self.MONEY_VOLUME / self.STEP_COUNT

        self.EXCHANGES_FOR_IN = position_config.EXCHANGES_FOR_IN

        self.del_pos_func = del_pos_func

        self.one_enter_step_in_procent = (self.MAX_UNCORRELATION_VALUE - self.ENTER_UNCORRELATION_VALUE) / self.STEP_COUNT
        self.WAIT_FOR_STEP = position_config.WAIT_FOR_STEP
        self.enter_steps_done_count = 0
        self.exit_steps_done_count = 0

        self.position_state_for_dispatcher = '' # close / cancel / active / wait_for_start
    
    async def start_work(self):

        symbol, stock1, stock2 = self.pair_key

        """
        ДлЯ тестов разрешено выполнять сделки даже если какая то биржа не готова
        """

        if stock1 not in self.EXCHANGES_FOR_IN or stock2 not in self.EXCHANGES_FOR_IN:
            await self.del_pos_func(self.pair_key)
            return
        if stock1 not in self.EXCHANGES_FOR_IN and stock2 not in self.EXCHANGES_FOR_IN:
            await self.del_pos_func(self.pair_key)
            return

        self.logger.debug(f'[POSITION SYSTEM] Намёк на позицию, задержка перед стартом - {self.WAIT_POSITION}.'
                          f'Пара - {self.pair_key}')

        self.position_state_for_dispatcher = 'wait_for_start'

        while time.time() - self.start_time < self.WAIT_POSITION:
            if self.uncorrelation_value < self.ENTER_UNCORRELATION_VALUE:
                self.position_state_for_dispatcher = 'cancel'
                self.logger.debug(f'[POSITION SISTEM] Позиция не стартовала - {self.pair_key}')
                await self.del_pos_func(self.pair_key)
                return
            self.last_uncorrelation_value = self.uncorrelation_value

            await asyncio.sleep(1)

        self.position_state_for_dispatcher = 'active'

        self.logger.success(f'[POSITION SYSTEM] {self.pair_key} - старт позиции ')

        await self.alert_queue.put(
            Notification(
                alert_type='start',
                symbol=self.symbol,
                uncorrelation=self.uncorrelation_value,
                total_volume=self.MONEY_VOLUME,
                part_volume=self.money_for_one_step,
                sell_exch=self.higher_exchange,
                buy_exch=self.lower_exchange,
                total_steps=self.STEP_COUNT,
                step=None,
            )
        )

        while True:

            one_exit_step_in_procent = (
                (self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[1] - self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[0])
                /
                max(1, self.enter_steps_done_count)
            )

            needed_uncor_for_exit_step = (
                self.UNCORRELATION_VALUE_RANGE_FOR_EXIT[1]
                - self.exit_steps_done_count * one_exit_step_in_procent
            )

            open_steps_now = self.enter_steps_done_count - self.exit_steps_done_count

            self.needed_uncor_for_enter_step = self.ENTER_UNCORRELATION_VALUE + open_steps_now * self.one_enter_step_in_procent

            is_enter = (
                self.uncorrelation_value >= self.needed_uncor_for_enter_step and
                self.enter_steps_done_count < self.STEP_COUNT
            )

            is_exit = (
                self.uncorrelation_value <= needed_uncor_for_exit_step and
                self.exit_steps_done_count < self.enter_steps_done_count
                        )

            try:
                
                if is_enter:

                    # ---------- ENTER ----------
                    order_data = {
                        'pair_key': self.pair_key,
                        'symbol': self.symbol,
                        'stage': 'enter',
                        'uncorrelation': self.uncorrelation_value,
                        'step': self.enter_steps_done_count + 1,
                        'steps_count': self.STEP_COUNT,
                        'volume': self.money_for_one_step,
                        'sell_exchange': {
                            'exchange': self.higher_exchange,
                            'price': self.higher_price,
                            # продаём на более дорогой — это SELL (верхняя биржа), posSide для неё = short (мы создаём шорт позицию там)
                            'side': 'SELL',
                            'posSide': 'short'
                        },
                        'buy_exchange': {
                            'exchange': self.lower_exchange,
                            'price': self.lower_price,
                            # покупаем на дешёвой — BUY (нижняя биржа), posSide для неё = long (мы открываем лонг там)
                            'side': 'BUY',
                            'posSide': 'long'
                        }
                    }

                    result = await self.order_manager.make_operation(
                        order_data
                        )
                    if result is None:
                        self.logger.warning('[POSITION SYSTEM]' \
                                            'Шаг входа не выполнен')
                        await asyncio.sleep(20)
                        await self.del_pos_func(self.pair_key)
                        return
                    else:
                        self.enter_steps_done_count += 1

                        self.logger.info(
                            f'[POSITION SYSTEM] Шаг входа - {self.pair_key}.'
                            f'Раскор - {self.uncorrelation_value}'
                            )
                        await self.alert_queue.put(
                            Notification(
                                alert_type='enter',
                                symbol=self.symbol,
                                uncorrelation=self.uncorrelation_value,
                                total_volume=self.MONEY_VOLUME,
                                part_volume=self.money_for_one_step,
                                sell_exch=self.higher_exchange,
                                buy_exch=self.lower_exchange,
                                step=self.enter_steps_done_count,
                                total_steps=self.STEP_COUNT
                            )
                        )

                elif is_exit:

                    # ---------- EXIT ----------
                    order_data = {
                        'pair_key': self.pair_key,
                        'symbol': self.symbol,
                        'stage': 'exit',
                        'uncorrelation': self.uncorrelation_value,
                        'step': self.exit_steps_done_count + 1,
                        'steps_count': self.STEP_COUNT,
                        'volume': self.money_for_one_step,
                        # чтобы закрыть: на верхней бирже закрываем шорт => BUY (закупаем чтобы закрыть short)
                        'sell_exchange': {
                            'exchange': self.higher_exchange,
                            'price': self.higher_price,
                            'side': 'BUY',
                            'posSide': 'short'
                        },
                        # на нижней бирже закрываем лонг => SELL (продаём, чтобы закрыть long)
                        'buy_exchange': {
                            'exchange': self.lower_exchange,
                            'price': self.lower_price,
                            'side': 'SELL',
                            'posSide': 'long'
                        }
                    }

                    result = await self.order_manager.make_operation(
                        order_data
                        )
                    if result is None:
                        self.logger.warning('[POSITION SYSTEM] ' \
                                            'Шаг выхода не выполнен')
                        await asyncio.sleep(20)
                        await self.del_pos_func(self.pair_key)
                        return
                    else:
                        self.exit_steps_done_count += 1

                        self.logger.info(
                            f'[POSITION SYSTEM] Шаг выхода - {self.pair_key}.'
                            f'Раскор - {self.uncorrelation_value}'
                            )
                        await self.alert_queue.put(
                            Notification(
                                alert_type='exit',
                                symbol=self.symbol,
                                uncorrelation=self.uncorrelation_value,
                                total_volume=self.MONEY_VOLUME,
                                part_volume=self.money_for_one_step,
                                sell_exch=self.higher_exchange,
                                buy_exch=self.lower_exchange,
                                step=self.exit_steps_done_count,
                                total_steps=self.STEP_COUNT
                            )
                        )
                        

                        if self.enter_steps_done_count == self.exit_steps_done_count:
                            self.position_state_for_dispatcher = 'close'
                            self.logger.success(f'[POSITION SYSTEM] {self.pair_key} - Позиция закрыта ')
                            
                            await self.alert_queue.put(
                                Notification(
                                    alert_type='close',
                                    symbol=self.symbol,
                                    uncorrelation=self.uncorrelation_value,
                                    total_volume=self.MONEY_VOLUME,
                                    part_volume=self.money_for_one_step,
                                    sell_exch=self.higher_exchange,
                                    buy_exch=self.lower_exchange,
                                    step=None,
                                    total_steps=self.STEP_COUNT
                                )
                            )
                            await self.del_pos_func(self.pair_key)
                            return


            except Exception as error:
                self.logger.error(f'[POSITION SYSTEM] {self.pair_key} Ошибка в выполнении обработки состояния - {error}')
            await asyncio.sleep(1)
    
    async def set_uncorrelation(self, uncorrelation):
        self.uncorrelation_value = uncorrelation

    def __str__(self):
        return f'{self.pair_key}\nhigher exchange - {self.higher_exchange}\nlower exchange - {self.lower_exchange}\nuncor - {self.uncorrelation_value}'
        
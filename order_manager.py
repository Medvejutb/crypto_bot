import asyncio

class Order_manager:
    def __init__(self, logger, cache_manager):
        self.logger = logger
        self.cache_manager = cache_manager

    async def make_operation(self, order_data: dict):
        """Главная точка входа для всех операций"""
        stage = order_data['stage']

        if stage == 'enter':
            await self.make_enter_step(order_data)
        elif stage == 'exit':
            await self.make_exit_step(order_data)
        else:
            raise ValueError(f"[ORDER SYSTEM] Неизвестный stage: {stage}")

    async def make_enter_step(self, order_data: dict):
        """Вход в позицию: купить на дешёвой, продать на дорогой"""
        await self._execute_pair_orders(
            order_data,
            buy_ex=order_data['buy_exchange'],
            sell_ex=order_data['sell_exchange'],
            stage_name="Вход"
        )

    async def make_exit_step(self, order_data: dict):
        """Выход из позиции: купить на дорогой, продать на дешёвой"""
        await self._execute_pair_orders(
            order_data,
            buy_ex=order_data['sell_exchange'],
            sell_ex=order_data['buy_exchange'],
            stage_name="Выход"
        )

    async def _execute_pair_orders(self, order_data, buy_ex, sell_ex, stage_name):
        """Общий метод для входа/выхода"""
        pair_key = order_data['pair_key']
        symbol = order_data['symbol']
        volume = order_data['volume']
        uncorrelation = order_data['uncorrelation']

        try:
            results = await asyncio.gather(
                self.execute_buy_order(symbol=symbol, exchange=buy_ex, volume=volume),
                self.execute_sell_order(symbol=symbol, exchange=sell_ex, volume=volume),
                return_exceptions=True
            )

            # Проверяем, не вернулось ли исключение
            for res in results:
                if isinstance(res, Exception):
                    raise res

            # self.logger.info(f"[ORDER SYSTEM] {stage_name} шаг {pair_key} "
            #                  f"- {uncorrelation} | {volume} {symbol} "
            #                  f"(buy {buy_ex}, sell {sell_ex})")

        except Exception as error:
            self.logger.error(f"[ORDER SYSTEM] Ошибка при {stage_name} {pair_key}: {error}")

    async def execute_buy_order(self, symbol, exchange, volume):
        """Заглушка: тут будет вызов API биржи"""
        self.logger.info(f"[ORDER SYSTEM] BUY {symbol} на {exchange} - {volume}")

    async def execute_sell_order(self, symbol, exchange, volume):
        """Заглушка: тут будет вызов API биржи"""
        self.logger.info(f"[ORDER SYSTEM] SELL {symbol} на {exchange} - {volume}")

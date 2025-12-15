import asyncio
from decimal import Decimal


class Order_manager:
    def __init__(
            self,
            logger,
            cache_manager,
            order_funcs,
            order_conf,
            ):
        self.logger = logger
        self.cache_manager = cache_manager
        self.order_funcs = {
            "binance": order_funcs['binance'],
            "bitget": order_funcs['bitget'], 
            "okx": order_funcs['okx'],
            "bybit": None,
            "gate": None,
        }
        self.conf = order_conf

    async def make_operation(
            self,
            order_data: dict
            ):
        stage = order_data.get("stage")

        if stage == "enter":
            execute = await self._execute_pair_orders(
                order_data,
                buy_ex=order_data["buy_exchange"],
                sell_ex=order_data["sell_exchange"],
                stage_name="Вход",
            )
            if execute:
                return True
        elif stage == "exit":
            execute = await self._execute_pair_orders(
                order_data,
                buy_ex=order_data["buy_exchange"],
                sell_ex=order_data["sell_exchange"],
                stage_name="Выход",
            )
            if execute:
                return True
        else:
            raise ValueError(f"[ORDER SYSTEM] Неизвестный stage: {stage}")

    async def _execute_pair_orders(
            self,
            order_data,
            buy_ex,
            sell_ex,
            stage_name
            ):
        """Общий метод для входа/выхода"""
        try:
            results = await asyncio.gather(
                self._execute_order(
                    side=buy_ex["side"].upper(),
                    order_data=order_data,
                    exchange=buy_ex["exchange"],
                    price=Decimal(str(buy_ex["price"])),
                    posSide=buy_ex.get("posSide"),
                ),
                self._execute_order(
                    side=sell_ex["side"].upper(),
                    order_data=order_data,
                    exchange=sell_ex["exchange"],
                    price=Decimal(str(sell_ex["price"])),
                    posSide=sell_ex.get("posSide"),
                ),
                return_exceptions=True,
            )

            for res in results:
                if isinstance(res, Exception):
                    raise res
            return True

        except Exception as error:
            self.logger.error(
                f"[ORDER SYSTEM] Ошибка при {stage_name} {order_data['pair_key']}: {error}"
            )

    async def _execute_order(
            self,
            side,
            order_data,
            exchange,
            price,
            posSide
            ):
        symbol = order_data["symbol"]
        volume = order_data["volume"]
        func = self.order_funcs.get(exchange)
        price = Decimal(price)

        if self.conf.get('fake_order'):
            self.logger.info(
                f"[ORDER SYSTEM] FAKE FAKE {side} {symbol} на {exchange} - {volume} FAKE FAKE"
            )
            return True

        if func is None:
            self.logger.warning(
                f"[ORDER SYSTEM] {side} {symbol} на {exchange} пока не реализован"
            )
            return

        try:
            result = await func(
                side=side,
                symbol=symbol,
                volume=volume,
                price=price,
                posSide=posSide,
            )


            if result is None:
                return result
            # else:
            #     self.logger.info(
            #         f"[ORDER SYSTEM] {side} {symbol} на {exchange} - {volume}, "
            #         f'response - {result}'
            #     )
        except Exception as e:
            self.logger.error(
                f"[ORDER SYSTEM] Ошибка {side} {symbol} на {exchange}: {e}"
            )
            raise

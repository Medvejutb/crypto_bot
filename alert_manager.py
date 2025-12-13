import asyncio
import msgspec
from datetime import datetime

from utils.math_operations import Calculator
from config import alert_config


class Alert_manager:
    def __init__(self, logger, bot, chat_id, interval, cache_manager):
        self.logger = logger
        self.bot = bot
        self.chat_id = chat_id
        self.interval = interval
        self.cache_manager = cache_manager
        self.alert_settings = {
            'uncorrelations': alert_config.uncorrelations,
            'positions': alert_config.positions,
        }
        self.alert_tasks = {
            'uncorrelations': self._uncor_alerting,
            'positions': self._pos_alerting,
        }
        self.alert_workers = {
            'uncorrelations': None,
            'positions': None,
        }
        self.pos_queue = asyncio.Queue()

    async def run(self, sockets_event):
        await sockets_event.wait()
        for name, value in self.alert_settings.items():
            if value:
                self.alert_workers[name] = asyncio.create_task(
                    self.alert_tasks[name]()
                    )

    async def _pos_alerting(self):
        """        
        TODO: Обще количество шагов выхода должно соответствовать количеству 
        выполнненных шагов входа, иначе путаница при сообщении о закрытии позиции, 
        когда был всего один шаг входа
        """
        while True:
            pos_event = await self.pos_queue.get()
            event_type = pos_event.alert_type
            msg_text = []
            
            if event_type == 'start':

                msg_text = "\n".join([
                    f'Старт позиции - {pos_event.symbol}',
                    f'{pos_event.sell_exch} | {pos_event.buy_exch}',
                    f'Раскорреляция - {Calculator.smart_round(
                        pos_event.uncorrelation
                        )}',
                    f'Объём на позицию - ${pos_event.total_volume}',
                    f'Всего шагов - {pos_event.total_steps}'
                ])
            elif event_type == 'enter':
                msg_text = "\n".join([
                    f'Шаг входа на {Calculator.smart_round(
                        pos_event.part_volume
                        )} - {pos_event.symbol} -'
                    f' {pos_event.step}/{pos_event.total_steps}',
                    f'Sell {pos_event.sell_exch} | Buy {pos_event.buy_exch}',
                    f'Раскорреляция - {Calculator.smart_round(
                        pos_event.uncorrelation
                        )}',
                ])
            elif event_type == 'exit':
                msg_text = "\n".join([
                    f'Шаг выхода на {Calculator.smart_round(
                        pos_event.part_volume
                        )} - {pos_event.symbol} -'
                    f' {pos_event.step}/{pos_event.total_steps}',
                    f'Buy {pos_event.sell_exch} | Sell {pos_event.buy_exch}',
                    f'Раскорреляция - {Calculator.smart_round(
                        pos_event.uncorrelation
                        )}',
                ])
            elif event_type == 'close':
                msg_text = "\n".join([
                    f'Позиция закрыта - {pos_event.symbol}',
                    f'{pos_event.sell_exch} | {pos_event.buy_exch}',
                    f'Объём на позицию - ${pos_event.total_volume}',
                ])
            await self.bot.send_message(
                self.chat_id,
                msg_text,
            )

    async def _uncor_alerting(self):
        """
        TODO: мелкие недочеты в представлений уведомлений:
        - полоска между раскорами есть даже когда есть всего один символ
        - фандинги странно округляются если ваще округляются
        - убедиться, что всё остальное корректно отображается
        """
        try:
            while True:
                uncorrelations =  await self.cache_manager.get_uncor()
                if uncorrelations:
                    self.logger.success('=======СООБЩЕНИЕ БОТА========')
                    message_dict = uncorrelations
                    message_text = []
                    message_text.append('==================')
                    for symbol, data in message_dict.items():
                        try:
                            message_text.append("\n".join([
                                f"{symbol} | РАСКОРРЕЛЯЦИЯ: {float(data['difference']):.2f}%",
                                f"🔺 Цена выше на {data['higher_exchange']}: {Calculator.smart_round(float(data['higher_price']))}",
                                f"🔻 Цена ниже на {data['lower_exchange']}: {Calculator.smart_round(float(data['lower_price']))}",
                                f"📊 {data['higher_exchange']}: funding -> {Calculator.smart_round(float(data['higher_funding']))}, {self._get_human_time(data['higher_next'])}",
                                f"📊 {data['lower_exchange']}: funding -> {Calculator.smart_round(float(data['lower_funding']))}, {self._get_human_time(data['lower_next'])}",
                                f"{'__________________________' if len(message_dict.keys()) < 2 else ''}"
                            ]))
                        except TypeError as typeerror:
                            self.logger.warning(f'Какая то неведомая хуйня пролезла в данные: {typeerror}')
                        except Exception as error:
                            self.logger.error(f'[ALERT MANAGER] Ошибка - {error}')
                    message_text = self._check_and_split_msg(message_text)
                    for text in message_text:
                        text = '\n'.join(text)
                        await self.bot.send_message(self.chat_id, text)
                    await asyncio.sleep(self.interval)
                else:
                    await asyncio.sleep(5)
        except Exception as error:
            self.logger.error(f'[ALERT MANAGER] Ошибка уведомления раскорреляций - {error}')

    def _check_and_split_msg(self, msg):
        chunks = []

        if len(msg) > 10:
            for i in range(0, len(msg), 10):
                block = msg[i:i + 10]
                block.append('==================')
                chunks.append(block)
        else:
            block = msg.copy()
            block.append('==================')
            chunks.append(block)

        return chunks

    def _get_human_time(self, future) -> str:
        if future is None:
            return future
        future = float(future)
        future_dt = datetime.fromtimestamp(int(future) / 1000)
        now = datetime.now()
        delta = future_dt - now

        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)

        return f"{days}д {hours}ч {minutes}м"
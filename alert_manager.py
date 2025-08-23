import asyncio

from utils.normalization import smart_round


class Alert_manager:
    def __init__(self, logger, bot, chat_id, interval, cache_manager):
        self.logger = logger
        self.bot = bot
        self.chat_id = chat_id
        self.interval = interval
        self.cache_manager = cache_manager

    
    async def start_alerting(self, sockets_event, get_uncorrelations_func):

        await sockets_event.wait()

        while True:

            # uncorrelations = get_uncorrelations_func()
            uncorrelations =  await self.cache_manager.get_uncor()
            if uncorrelations:
                self.logger.success('=======СООБЩЕНИЕ БОТА========')
                message_dict = uncorrelations
                message_text = []
                for symbol, data in message_dict.items():
                    try:
                        message_text.append("\n".join([
                            f"{symbol} | РАСКОРРЕЛЯЦИЯ: {data['difference']:.2f}%",
                            f"🔺 Цена выше на {data['higher_exchange']}: {smart_round(data['higher_price'])}",
                            f"🔻 Цена ниже на {data['lower_exchange']}: {smart_round(data['lower_price'])}",
                            f"📊 {data['higher_exchange']}: price -> {smart_round(data['higher_price'])}, funding -> {smart_round(float(data['higher_funding']))}, {data['higher_next']}",
                            f"📊 {data['lower_exchange']}: price -> {smart_round(data['lower_price'])}, funding -> {smart_round(float(data['lower_funding']))}, {data['lower_next']}",
                            f"__________________________"
                        ]))
                    except TypeError as typeerror:
                        self.logger.worning(f'Какая то неведомая хуйня пролезла в данные: {typeerror}')
                message_text = self._check_and_split_msg(message_text)
                for text in message_text:
                    text = '\n'.join(text)
                    await self.bot.send_message(self.chat_id, text)
                await self.bot.send_message(self.chat_id, '==========КОНЕЦ СООБЩЕНИЯ==========')
                await asyncio.sleep(self.interval)
            else:
                # self.logger.info('====================СООБЩЕНИЕ БОТА ПУСТОЕ=====================')
                await asyncio.sleep(5)

    
    def _check_and_split_msg(self, msg):
        if len(msg) > 10:
            chunks = [msg[i:i + 10] for i in range(0, len(msg), 10)]
            return chunks
        else:
            return [msg]
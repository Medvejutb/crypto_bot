# bot.py
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from controller import Controller
import config
from pprint import pprint
from utils.normalization import join_stocks_dicts_to_main_stocks_dict
from utils.normalization import smart_round

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

ctrl = Controller()

@dp.message()
async def get_id(message: Message):
    await message.answer(f"Твой айди\n{message.from_user.id}")

async def background_worker():
    await ctrl.start_all_sockets()
    while True:
        symbols_prices = ctrl.get_needed_symbols_with_prices()
        symbols_fundings = await ctrl.get_fundings_for_symbols(symbols_prices)
        main_dict = join_stocks_dicts_to_main_stocks_dict(symbols_prices, symbols_fundings)
        uncorrelations = ctrl.get_uncorrelations(main_dict)
        print('====================СООБЩЕНИЕ БОТА=====================')
        message_dict = uncorrelations

        message_text = []

        for symbol, data in message_dict.items():
            message_text.append("\n".join([
                f"{symbol} | РАСКОРРЕЛЯЦИЯ: {data['difference']:.2f}%",
                f"🔺 Цена выше на {data['higher_exchange']}: {smart_round(data['higher_price'])}",
                f"🔻 Цена ниже на {data['lower_exchange']}: {smart_round(data['lower_price'])}",
                f"📊 {data['higher_exchange']}: price -> {smart_round(data['higher_price'])}, funding -> {smart_round(data['higher_funding'])}, {data['higher_next']}",
                f"📊 {data['lower_exchange']}: price -> {smart_round(data['lower_price'])}, funding -> {smart_round(data['lower_funding'])}, {data['lower_next']}",
                f"__________________________"
            ]))

        print(len(message_text))

        message_text = '\n'.join(message_text)


        await bot.send_message(5608629096, message_text)

        await bot.send_message(5608629096, '====================')

        await asyncio.sleep(config.CHECK_INTERVAL)

async def main():
    asyncio.create_task(background_worker())
    await dp.start_polling(bot)


asyncio.run(main())
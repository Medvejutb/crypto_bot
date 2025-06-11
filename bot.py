import telebot
import config
import controller
from time import sleep
from utils.normalization import smart_round,get_human_time
from pprint import pprint

bot = telebot.TeleBot(config.BOT_TOKEN)

@bot.message_handler(func=lambda message: True)
def hi(message):
    bot.send_message(message.chat.id, f'Твой айди\n{message.from_user.id}')

while True:

    message_dict = controller.get_and_compare_from_stocks(config.START_STOCK, config.EXCHANGES)

    for data_stocks in message_dict:
        for symbol, data in data_stocks.items():

            message_text = "\n".join([
                f"{symbol} | РАСКОРРЕЛЯЦИЯ: {data['difference']:.2f}%",
                f"🔺 Цена выше на {data['higher_exchange']}: {smart_round(data['higher_price'])}",
                f"🔻 Цена ниже на {data['lower_exchange']}: {smart_round(data['lower_price'])}",
                f"📊 {data['higher_exchange']}: price -> {smart_round(data['higher_price'])}, funding -> {smart_round(data['higher_funding'])}, {data['higher_next']}",
                f"📊 {data['lower_exchange']}: price -> {smart_round(data['lower_price'])}, funding -> {smart_round(data['lower_funding'])}, {data['lower_next']}"
            ])

            bot.send_message(5608629096, message_text)

    bot.send_message(5608629096, '------------------------')
    print('[SYSTEM] Задержка...')
    sleep(config.CHECK_INTERVAL)


bot.polling()
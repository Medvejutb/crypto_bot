import telebot
from config import BOT_TOKEN
from exchanges import binance, bitget
from pathlib import Path
from utils import calc, normalization
from time import sleep


bot = telebot.TeleBot(BOT_TOKEN)

BASE_DIR = Path(__file__).resolve().parent.parent
binance_path = BASE_DIR / 'exchanges' / 'binance_funding_and_price.json'
bitget_path = BASE_DIR / 'exchanges' / 'bitget_funding_and_price.json'

"""
with open(bitget_path, 'r') as file:
    data_bitget = json.load(file)
with open(binance_path, 'r') as file:
    data_binance = json.load(file)
"""

def smart_round(price: float) -> str:
    if price > 100:
        return f"{price:.0f}"
    elif price > 1:
        return f"{price:.2f}"
    elif price > 0.01:
        return f"{price:.3f}"
    else:
        return f"{price:.6f}"


@bot.message_handler(func=lambda message: True)
def hi(message):
    bot.send_message(message.chat.id, f'Твой айди\n{message.from_user.id}')


while True:
    print('[SYSTEM] Парсинг бирж...')
    data_binance = binance.join_price_funding()
    data_bitget = bitget.get_price_funding()
    print('[SYSTEM] Обработка данных')

    message_dict = calc.calc(data_binance, data_bitget)

    for symbol, data in message_dict.items():

        next_funding_time_on_bitget = bitget.get_next_funding(symbol)

        next_funding_time_on_bitget = normalization.get_human_time(next_funding_time_on_bitget)

        if data['higher_exchange'] == 'bitget':
            data['higher_next'] = next_funding_time_on_bitget
        elif data['lower_exchange'] == 'bitget':
            data['lower_next'] = next_funding_time_on_bitget

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
    sleep(21)



bot.polling()
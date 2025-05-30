from config import THRESHOLD
from utils import normalization
from pprint import pprint
from config import EXCHANGES #ЭТО КОСТЫЛЬ

"""
Тут походу всё нахуй надо переделывать
"""
def calc(stock1, stock2) -> list:
    data_coins = normalization.join(stock1, stock2)

    stock_name1, stock_name2 = list(data_coins[next(iter(data_coins))].keys())

    difference = {}

    for symbol, data in data_coins.items():
        if data[stock_name1]['price'] <= data[stock_name2]['price']:

            lower_exchange, higher_exchange = list(data.keys())

            higher_price = data[stock_name2]['price']
            lower_price = data[stock_name1]['price']
            higher_funding = data[stock_name2]['funding']
            lower_funding = data[stock_name1]['funding']
            higher_next_funding = data[stock_name2]['next_funding_time']
            lower_next_funding = data[stock_name1]['next_funding_time']

        else:

            higher_exchange, lower_exchange = list(data.keys())

            higher_price = data[stock_name1]['price']
            lower_price = data[stock_name2]['price']
            higher_funding = data[stock_name1]['funding']
            lower_funding = data[stock_name2]['funding']
            higher_next_funding = data[stock_name1]['next_funding_time']
            lower_next_funding = data[stock_name2]['next_funding_time']

        middle_price = (higher_price + lower_price) / 2
        spread = (higher_price - lower_price) / middle_price * 100

        if abs(spread) >= THRESHOLD:
            difference[symbol] = {
                    'difference': spread,
                    'higher_price': higher_price,
                    'lower_price': lower_price,
                    'higher_exchange': higher_exchange,
                    'lower_exchange': lower_exchange,
                    'higher_next': higher_next_funding,
                    'lower_next': lower_next_funding,
                    'higher_funding': higher_funding,
                    'lower_funding': lower_funding,
                }


    return difference


"""
    for symbol, data in data_coins.items():

        price1 = data['stock1']['price']
        price2 = data['stock2']['price']

        middle_price = (price1 + price2) / 2
        spread = (price1 - price2) / middle_price * 100

        #pprint(data_coins)

        if price1 > price2: # КОСТЫЛЬНЫЕ ХАЙГЕР И ЛОУВЕР ЧЕЙНДЖИ
            higher_exchange = EXCHANGES[2]
            lower_exchange = EXCHANGES[0]
            higher_price = price1
            lower_price = price2
        else:
            higher_exchange = EXCHANGES[0]
            lower_exchange = EXCHANGES[2]
            higher_price = price2
            lower_price = price1

        print(f"{symbol} | price1: {price1}, price2: {price2}")

        if abs(spread) >= THRESHOLD:

            symbol_list.append([f'{symbol} | РАСКОРРЕЛЯЦИЯ: {spread:.2f}%',
                    f'🔺 Цена выше на {higher_exchange}: {higher_price}',
                    f'🔻 Цена ниже на {lower_exchange}: {lower_price}',
                    f'📊 {higher_exchange}: price - {data["stock1"]["price"]}, funding - {data["stock1"]["funding"]}, [NEXT FUNDING]',
                    f'📊 {lower_exchange}: price - {data["stock2"]["price"]}, funding - {data["stock2"]["funding"]}, [NEXT FUNDING]'
                    ])
    pprint(symbol_list)

    return symbol_list
"""
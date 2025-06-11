from config import THRESHOLD
from utils import normalization

def calc(stock1, stock2) -> dict:
    data_coins = normalization.join_stocks_to_dict(stock1, stock2)

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
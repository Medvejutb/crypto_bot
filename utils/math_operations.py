import asyncio
import config
from utils.normalization import join_stocks_to_dict


class Calculator:
    def __init__(self):
        pass

    def calc_uncorrelation(self, stock1, stock2, stock_name1, stock_name2):
        try:
            higher_price, lower_price = None, None
            higher_exchange, lower_exchange = None, None
            higher_funding, lower_funding = None, None
            higher_next, lower_next = None, None

            if stock1['price'] >= stock2['price']:
                higher_price, lower_price = stock1['price'], stock2['price']
                higher_exchange, lower_exchange = stock_name1, stock_name2
                higher_funding, lower_funding = stock1['funding'], stock2['funding']
                higher_next, lower_next = stock1['next_funding_time'], stock2['next_funding_time']
            else:
                higher_price, lower_price = stock2['price'], stock1['price']
                higher_exchange, lower_exchange = stock_name2, stock_name1
                higher_funding, lower_funding = stock2['funding'], stock1['funding']
                higher_next, lower_next = stock2['next_funding_time'], stock1['next_funding_time']

            middle_price = (higher_price + lower_price) / 2
            spread = (higher_price - lower_price) / middle_price * 100

            if abs(spread) >= config.UNCORRELATION_PARA:
                return {
                    'difference': spread,
                    'higher_price': higher_price,
                    'lower_price': lower_price,
                    'higher_exchange': higher_exchange,
                    'lower_exchange': lower_exchange,
                    'higher_next': higher_next,
                    'lower_next': lower_next,
                    'higher_funding': higher_funding,
                    'lower_funding': lower_funding,
                }

            return None
        except Exception as e:
            print(f'[ERROR] При расчёте раскорреляции: {e}')
            return None

    def calc_treshold(self, stock1, stock2):
        data_coins = join_stocks_to_dict(stock1, stock2)
        stock_name1, stock_name2 = list(data_coins[next(iter(data_coins))].keys())
        treshold = {}

        for symbol, data in data_coins.items():

            price_1 = data[stock_name1]['price']
            price_2 = data[stock_name2]['price']

            spread = ((price_1 - price_2) / price_2) * 100

            treshold[symbol] = spread

    def calc_spread(self, price_1, price_2):
        spread = ((price_1 - price_2) / price_2) * 100
        return spread

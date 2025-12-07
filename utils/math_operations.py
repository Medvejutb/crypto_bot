import asyncio
from decimal import Decimal
import config
from utils.normalization import join_stocks_to_dict


D100 = Decimal('100')
D2 = Decimal('2')


class Calculator:
    def __init__(self):
        pass

    def calc_uncorrelation(self, stock1, stock2, exch1, exch2, active_pos=False):
        try:
            higher, lower = (stock1, stock2) if stock1['price'] >= stock2['price'] else (stock2, stock1)
            spread = self.calc_spread(higher['price'], lower['price'])

            if abs(spread) < Decimal(str(config.UNCORRELATION_PARA)) and not active_pos:
                return None

            return {
                'difference': str(spread),
                'higher_price': str(higher['price']),
                'lower_price': str(lower['price']),
                'higher_exchange': exch1 if higher == stock1 else exch2,
                'lower_exchange': exch1 if lower == stock1 else exch2,
                'higher_next': str(higher.get('next_funding_time')),
                'lower_next': str(lower.get('next_funding_time')),
                'higher_funding': str(higher.get('funding')),
                'lower_funding': str(lower.get('funding')),
            }

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

            spread = (price_1 - price_2) / price_2 * D100   # Decimal

            treshold[symbol] = spread

        return treshold

    def calc_spread(self, price_1, price_2):
        return (price_1 - price_2) / price_2 * D100

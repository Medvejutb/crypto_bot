from exchanges import binance, bitget
import config
from utils.calc import calc
from utils.normalization import get_human_time
from pprint import pprint
from utils import checker
import asyncio# на следующем обновлений



def get_and_compare_from_stocks(START_STOCK, EXCHANGES):

    print('[SYSTEM] Парсинг бирж...')
    try:
        exchange_data = {
            'binance': binance.join_price_funding(),
            'bitget': bitget.get_price_funding(),
            'bybit': None,
            'okx': None,
            'gate': None
        }
    except '[SYSTEM] Возникла ошибка при парсинге' as error:
        print(error)
        return

    print('[SYSTEM] Обработка данных')

    uncorrelations_dict = {}
    uncorrelations_list = []

    for stock in EXCHANGES:
        if stock == START_STOCK or exchange_data[stock] is None:
            continue
        start_stock = exchange_data[START_STOCK]
        other_stock = exchange_data[stock]
        uncorrelation = calc(start_stock, other_stock)
        uncorrelations_dict[f'{START_STOCK}-{stock}'] = uncorrelation

        uncorrelations_list.append(uncorrelation)

    #cleaned_data = checker.check_more_uncorrelations_from_results(uncorrelations_dict)

    for symbols_dict in uncorrelations_list:
        for symbol, data in symbols_dict.items():
            if data['higher_exchange'] == 'bitget':
                data['higher_next'] = get_human_time(bitget.get_next_funding(symbol))
                data['lower_next'] = get_human_time(data['lower_next'])
            elif data['lower_exchange'] == 'bitget':
                data['lower_next'] = get_human_time(bitget.get_next_funding(symbol))
                data['higher_next'] = get_human_time(data['higher_next'])


    return uncorrelations_list
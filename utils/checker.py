from pprint import pprint

"""
Функция сравнивает раскорреляции активов с разных пар бирж
"""

def check_more_uncorrelations_from_results(uncorrelation_dict) -> dict:

    symbols_n_uncorr = {}
    uncorr_list = []

    for stocks_pair, symbols in uncorrelation_dict.items():
        pprint(f'БИРЖИ - {stocks_pair}')
        pprint(symbols)
        for current_symbol, data in symbols.items():
            f'{stocks_pair}'
            uncorr_list.append(data['difference'])


    pprint(uncorr_list)
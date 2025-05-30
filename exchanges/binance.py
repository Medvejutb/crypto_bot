import requests
import json
from time import sleep
from pprint import pprint

"""
Функция собирает прайсы, фандинги и время до следующего фандинга для каждого symbol
и кидает в файл json.
"""

def get_coins_with_status_TRADING() -> list:
    url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    response = requests.get(url)
    data = response.json()

    return_list = []

    for item in data['symbols']:
        if (item.get('status') == 'TRADING'
            and item.get('contractType') == 'PERPETUAL'
            and item.get('quoteAsset') == 'USDT'
        ):
            return_list.append(item.get('pair'))

    return return_list

def get_coins_price(TRADING_LIST) -> dict:
    url = 'https://fapi.binance.com/fapi/v1/ticker/price'
    response = requests.get(url)
    data = response.json()

    price_dict = {}

    for item in data:
        if item.get('symbol') in TRADING_LIST:
            price_dict[item.get('symbol')] = {
                'price': item.get('price')
            }
        #else:
        #    print(f'[SYSTEM] Актив {item.get('symbol')} не торгуется, нихуя себе....')

    return price_dict

def get_coins_funding(TRADING_LIST) -> dict:
    url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    response = requests.get(url)
    data = response.json()

    funding_dict = {}

    for item in data:
        if item.get('symbol') in TRADING_LIST:
            funding_dict[item.get('symbol')] = {
                'funding': float(item.get('lastFundingRate', 0)),
                'next_funding_time': int(item.get('nextFundingTime', 0))
            }
    return funding_dict

def join_price_funding() -> dict:
    trading_list = get_coins_with_status_TRADING()

    price_dict = get_coins_price(trading_list)
    funding_dict = get_coins_funding(trading_list)

    binance_dict = {}

    for symbol, funding_data in funding_dict.items():
        if symbol in price_dict:
            binance_dict[symbol] = {
                'price': float(price_dict[symbol]['price']),
                'funding': funding_data['funding'],
                'next_funding_time': funding_data['next_funding_time']
            }

    binance_dict['stock'] = 'binance'

    return binance_dict

"""
Следующая функция из старой версии
"""

"""
def get_fundings_and_price_from_api_to_json() -> dict:
    url_for_price = 'https://fapi.binance.com/fapi/v1/ticker/price'
    url_for_funding = 'https://fapi.binance.com/fapi/v1/premiumIndex'

    try:
        print('[BINANCE] getting prices...')
        response_price = requests.get(url_for_price)
        response_price.raise_for_status()
        print('[BINANCE]...YES')

        sleep(1)

        print('[BINANCE] getting fundings...')
        response_funding = requests.get(url_for_funding)
        response_funding.raise_for_status()
        print('[BINANCE] ...YES')

    except requests.RequestException as error:
        print(f'[BINANCE ERROR] Не смог получить данные: {error}')
        return

    data4price = response_price.json()
    data4funding = response_funding.json()
    new_data = {}

    price_dict = {item['symbol']: float(item['price']) for item in data4price}

    for item in data4funding:
        symbol = item['symbol']
        if symbol in price_dict:
            funding = float(item.get('lastFundingRate', 0))
            time = int(item.get('time', 0))
            next_funding_time = int(item.get('nextFundingTime', 0))

            new_data[symbol] = {
                'price': price_dict[symbol],
                'funding': funding,
                'next_funding_time': next_funding_time,
                'time': time
            }

    return new_data
"""
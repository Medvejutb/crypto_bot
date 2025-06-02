import requests

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

    return price_dict

def get_coins_funding(TRADING_LIST) -> dict:
    url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
    response = requests.get(url)
    data = response.json()

    funding_dict = {}

    for item in data:
        if item.get('symbol') in TRADING_LIST:
            funding_dict[item.get('symbol')] = {
                'funding': float(item.get('lastFundingRate', 0)) * 100,
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

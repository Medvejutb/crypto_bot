import requests

def get_price_funding() -> dict:
    url = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'

    try:
        print('[BITGET] getting data...')
        response = requests.get(url)
        response.raise_for_status()
        print('[BITGET] ...YES')

    except requests.RequestException as error:
        print(f'[BITGET ERROR] Не смог получить данные: {error}')
        return

    data = response.json()
    bitget_dict = {}

    for item in data['data']:
        symbol = item.get('symbol')
        if float(item.get('lastPr')) > 0:
            price = item.get('lastPr')
        else:
            continue
        funding = float(item.get('fundingRate')) * 100
        time = item.get('ts')
        next_funding_time = None

        bitget_dict[symbol] = {
            'price': price,
            'funding': funding,
            'next_funding_time': next_funding_time,
            'time': time
        }

    bitget_dict['stock'] = 'bitget'

    return bitget_dict

def get_next_funding(symbol) -> dict:
    url = 'https://api.bitget.com/api/v2/mix/market/funding-time'
    next_symbol_funding = {}
    params = {
        "symbol": symbol,
        "productType": "usdt-futures"
    }
    try:
        response = requests.get(url, params)
        response.raise_for_status()
    except requests.RequestException as error:
        print(f'[BITGET ERROR] Не смог получить данные: {error}')
        return

    data = response.json()

    next_symbol_funding = data['data'][0]['nextFundingTime']

    return next_symbol_funding

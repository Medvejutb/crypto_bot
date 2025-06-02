import requests
import json
from time import sleep

"""
Функция собирает прайсы, фандинги и время до следующего фандинга для каждого symbol
и кидает в файл json.
"""

def get_fundings_and_price_from_api_to_json():
    url_for_price = 'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
    url_for_funding = 'https://www.okx.com/api/v5/public/funding-rate?instType=FUTURES'

    try:
        print('getting prices...')
        response_price = requests.get(url_for_price)
        response_price.raise_for_status()
        print('...YES')

        sleep(1)

        """
        Фандинг каждого актива надо отдельно..........................
        """

        #print('getting fundings...')
        #response_funding = requests.get(url_for_funding)
        #response_funding.raise_for_status()
        #print('...YES')

    except requests.RequestException as error:
        print(f'[OKX ERROR] Не смог получить данные: {error}')
        return

    data4price = response_price.json()
    #data4funding = response_funding.json()
    new_data = {}

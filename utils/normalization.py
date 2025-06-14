from datetime import datetime

def smart_round(price: float) -> str:
    if float(price) > 100:
        return f"{price:.0f}"
    elif float(price) > 1:
        return f"{price:.2f}"
    elif float(price) > 0.01:
        return f"{price:.3f}"
    else:
        return f"{price:.6f}"


def get_human_time(future):
    if future is None:
        return future
    future_dt = datetime.fromtimestamp(int(future) / 1000)
    now = datetime.now()
    delta = future_dt - now

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    return f"{days}д {hours}ч {minutes}м"

def join_stocks_to_dict(stock1, stock2):
    data_coins = {}
    stock_name1 = stock1['stock']
    stock_name2 = stock2['stock']
    try:
        for symbol in stock1:
            if symbol == 'stock':
                continue

            if symbol in stock2:

                stock1_time = stock1[symbol]['next_funding_time']
                stock2_time = stock2[symbol]['next_funding_time']

                data_coins[symbol] = {
                    stock_name1: {
                        'price': float(stock1[symbol]['price']),
                        'funding': float(stock1[symbol]['funding']),
                        'next_funding_time': int(stock1[symbol]['next_funding_time']) if stock1_time is not None else 'HUI'
                    },
                    stock_name2: {
                        'price': float(stock2[symbol]['price']),
                        'funding': float(stock2[symbol]['funding']),
                        'next_funding_time': int(stock2[symbol]['next_funding_time']) if stock2_time is not None else 'HUI'
                    }
                }
    except Exception as error:
        print(f'[SYSTEM NORMALIZATION] Произошла ошибка в функции нормализации - {error}')

    return data_coins

def join_stocks_dicts_to_main_stocks_dict(symbols_prices, symbols_fundings):
    main_stocks_dict = {}

    for symbol, price_exchanges in symbols_prices.items():
        main_stocks_dict[symbol] = {}

        for exchange, price_data in price_exchanges.items():
            # Сразу собираем основу
            main_stocks_dict[symbol][exchange] = {
                'price': float(price_data['price']),
                'time': int(price_data['time']),
                'funding': None,
                'next_funding_time': None
            }

            funding_data = symbols_fundings.get(symbol, {}).get(exchange)
            if funding_data:
                main_stocks_dict[symbol][exchange]['funding'] = funding_data.get('funding')
                main_stocks_dict[symbol][exchange]['next_funding_time'] = get_human_time(funding_data.get('next_funding_time'))

    return main_stocks_dict

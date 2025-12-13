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
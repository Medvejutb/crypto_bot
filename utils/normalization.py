from datetime import datetime

def get_human_time(future):
    future_dt = datetime.fromtimestamp(int(future) / 1000)
    now = datetime.now()
    delta = future_dt - now

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    return f"{days}д {hours}ч {minutes}м"

def join(stock1, stock2, stock3=None, stock4=None):
    data_coins = {}
    stock_name1 = stock1['stock']
    stock_name2 = stock2['stock']

    for symbol in stock1:
        if symbol == 'stock':
            continue

        if symbol in stock2:

            time1_to = get_human_time(int(stock1[symbol]['next_funding_time']))
            time2_to = get_human_time(int(stock2[symbol]['next_funding_time'])) if stock2[symbol]['next_funding_time'] is not None else 'HUI'

            data_coins[symbol] = {
                stock_name1: {
                    'price': float(stock1[symbol]['price']),
                    'funding': float(stock1[symbol]['funding']),
                    'next_funding_time': time1_to
                },
                stock_name2: {
                    'price': float(stock2[symbol]['price']),
                    'funding': float(stock2[symbol]['funding']),
                    'next_funding_time': time2_to
                }
            }

    return data_coins

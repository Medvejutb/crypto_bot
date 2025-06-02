from datetime import datetime

def smart_round(price: float) -> str:
    if price > 100:
        return f"{price:.0f}"
    elif price > 1:
        return f"{price:.2f}"
    elif price > 0.01:
        return f"{price:.3f}"
    else:
        return f"{price:.6f}"


def get_human_time(future):
    future_dt = datetime.fromtimestamp(int(future) / 1000)
    now = datetime.now()
    delta = future_dt - now

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    return f"{days}д {hours}ч {minutes}м"

def join_stocks_to_dict(stock1, stock2, stock3=None, stock4=None):
    data_coins = {}
    stock_name1 = stock1['stock']
    stock_name2 = stock2['stock']

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

    return data_coins

'''
ручной импорт всех бирж
'''

EXCHANGE_REGISTRY = {}

from binance_socket import Binance
from bitget_socket import Bitget
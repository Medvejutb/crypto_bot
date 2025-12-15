import msgspec
from decimal import Decimal
from typing import Literal

exchanges = ['okx', 'binance', 'bitget']

class PairMetrick(msgspec.Struct):
    exch1: str
    exch2: str
    value: Decimal

class Position(msgspec.Struct):
    symbol: str
    higher_exchange: str
    lower_exchange: str
    start_time: int
    uncorrelation: Decimal
    higher_price: Decimal
    lower_price: Decimal
    state: Literal['wait', 'active', 'closing', 'closed', 'cancel']
    

class SymbolExchange(msgspec.Struct):
    exchange: str
    price: Decimal | None
    funding: Decimal | None
    next_funding: int | None

class Symbol(msgspec.Struct):
    symbol: str
    exchanges: dict[str, SymbolExchange] # тут будут храниться SymbolExchange по бирже
    spreads: dict[tuple[str, str], PairMetrick] | None # тут ключи вида (символ, биржа, биржа)
    uncorrelations: dict[tuple[str, str], PairMetrick] | None # тут ключи вида (символ, биржа, биржа)
    positions: dict[tuple[str, str], Position] | None # тут ключи вида (символ, биржа, биржа)
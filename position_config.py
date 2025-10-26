"""Настройки позиций"""

MAX_UNCORRELATION_VALUE = 2.5 # Максимальное значение раскорреляции в %

ENTER_UNCORRELATION_VALUE = 1.5 # value to enter position in %

UNCORRELATION_VALUE_RANGE_FOR_EXIT = (0.5, 1) # Диапазон для шагов выхода в %

MONEY_VOLUME = 50 # Объем на одну позицию в $

STEP_COUNT = 3 # Количество шагов входа и выхода на позицию

EXCHANGES_FOR_IN = ['okx', 'binance', 'bitget'] # Биржи, которые участвуют в сделке (одна в лонг, вторая в шорт)

MAX_COUNT_POSITIONS = 1 # Максимальное количество позиций

WAIT_POSITION = 15 # Задержка перед открытием позиции

WAIT_FOR_STEP = 1 # Задержка перед выполнением шага
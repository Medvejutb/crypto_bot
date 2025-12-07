import asyncio

from aiogram import Bot, Dispatcher
from aiogram.types import Message

import config
import position_config
from alert_manager import Alert_manager
from cache_manager import Cache_manager
from funding_manager import Funding_manager
from position_dispatcher import Position_dispatcher
from sockets_manager import Sockets_manager
from uncorrelation_manager import Uncorrelation_manager
from utils.views import Logging_manager
from order_manager import Order_manager

bot = Bot(token=config.BOT_TOKEN)

tg_dispatcher = Dispatcher()

logger = Logging_manager.get_logger()

cache_manager = Cache_manager()

sockets_manager = Sockets_manager(logger=logger, cache_manager=cache_manager)

position_and_alert_queue = asyncio.Queue()

funding_manager = Funding_manager(
    logger=logger,
    cache_manager=cache_manager,
    funding_funcs=sockets_manager.stocks_fundings_funcs,
    time_live_funding=config.TIME_LIVE_FUNDING_IN_MEMORY,
    )

uncorrelation_manager = Uncorrelation_manager(
        logger=logger,
        cache_manager=cache_manager,
        funding_manager=funding_manager,
        bot=bot,
        chat_id=config.CHAT_ID,
        interval=config.CHECK_INTERVAL,
        exchanges=config.EXCHANGES,
        spread=config.SPREAD,
        get_raw_sockets_data=sockets_manager.get_prices_from_exchanges,
        sockets_ready_event=sockets_manager.sockets_ready_event,
        )

alert_manager = Alert_manager(
    logger=logger,
    bot=bot,
    chat_id=config.CHAT_ID,
    interval=config.CHECK_INTERVAL,
    cache_manager=cache_manager,
    )

order_manager = Order_manager(
    cache_manager=cache_manager,
    logger=logger,
    order_funcs=sockets_manager.order_funcs,
    order_conf=config.ORDER_CONF,
)

positions_dispatcher = Position_dispatcher(
    cache_manager=cache_manager,
    order_manager=order_manager,
    logger=logger,
    position_config=position_config,
    )




@tg_dispatcher.message()
async def get_id(message: Message):
    await message.answer(f"Твой айди\n{message.from_user.id}")


async def ws_worker(queue):
    logger.success('[WEBSOCKET SYSTEM] Запуск сокетов...')
    await sockets_manager.start_all_sockets(queue=queue)

# async def funding_worker():
#     logger.success('[FUNDING SYSTEM] Запуск работы фандингов')


async def uncorrelation_worker(funding_queue):
    logger.success('[UNCORRELATION SYSTEM] Запуск расчетов раскорреляций')
    await uncorrelation_manager.start_work()


async def alert_worker(sockets_event):
    await alert_manager.start_alerting(
        sockets_event=sockets_event)
    

async def positions_worker():
    logger.success('[POSITION SYSTEM] Запуск работы с позициями')
    await positions_dispatcher.start_working(uncorrelation_manager.uncorrelations_event)




async def main():

    await cache_manager.clear()
    await asyncio.sleep(0.2)

    queue = asyncio.Queue()
    funding_queue = asyncio.Queue()

    asyncio.create_task(ws_worker(queue))
    await asyncio.sleep(0.2)

    # asyncio.create_task(funding_worker())

    asyncio.create_task(uncorrelation_worker(funding_queue))
    await asyncio.sleep(0.2)

    asyncio.create_task(alert_worker(sockets_event=sockets_manager.sockets_ready_event))
    await asyncio.sleep(0.2)

    asyncio.create_task(positions_worker())
    await asyncio.sleep(0.2)

    await tg_dispatcher.start_polling(bot)


asyncio.run(main())
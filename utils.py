import logging
from datetime import datetime, timedelta, time, date

from config import IST_OFFSET, HOLIDAYS

logger = logging.getLogger(__name__)

def is_market_open(now_ist=None, TESTING=False):
    if TESTING:
        logger.info("TESTING...")
        return True

    if not now_ist:
        now_ist = datetime.utcnow() + IST_OFFSET

    is_market_time = time(9, 15) <= now_ist.time() <= time(15, 30)

    if is_trading_day(now_ist.date()) and is_market_time:
        logger.info("Market is open...")
        return True

def get_last_trading_day(d: date) -> date:
    while d.weekday() >= 5 or d.isoformat() in HOLIDAYS:
        d -= timedelta(days=1)
    return d

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d.isoformat() not in HOLIDAYS

def is_pre_market_hours(now_ist: datetime) -> bool:
    pre_market_end = time(9, 0)
    current_time = now_ist.time()
    return current_time < pre_market_end
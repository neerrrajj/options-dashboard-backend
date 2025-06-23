import asyncio
import logging
from fastapi import FastAPI
from datetime import datetime, timedelta, time

from db import Base, engine
from config import HOLIDAYS
from processors.fetch_oc_snapshot import fetcher, IST_OFFSET

TESTING = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

Base.metadata.create_all(bind=engine)
app = FastAPI(title="Options Dashboard API")

logger = logging.getLogger(__name__)

def is_market_open(now_ist=None, TESTING=False):
    """Returns True if the given (or current) IST time is within market time"""
    if TESTING:
        logger.info("TESTING...")
        return True

    if not now_ist:
        now_ist = datetime.utcnow() + IST_OFFSET

    is_weekday = 0 <= now_ist.weekday() <= 4
    is_market_time = time(9, 15) <= now_ist.time() <= time(15, 30)
    is_holiday = now_ist.date().isoformat() in HOLIDAYS

    if is_weekday and is_market_time and not is_holiday:
        logger.info("Market is open...")
        return True

@app.on_event("startup")
async def start_fetcher():
    async def fetch_scheduler_loop():
        while True:
            now = datetime.utcnow() + IST_OFFSET

            if is_market_open(now, TESTING):
                asyncio.create_task(fetcher())
            # elif current_time > time(15, 30) and now.time() < time(23, 40):
            #     asyncio.create_task(is_closing_snapshot_needed())

            # Sleep until the next exact minute
            next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            sleep_duration = max((next_minute - (datetime.utcnow() + IST_OFFSET)).total_seconds(), 0)
            await asyncio.sleep(sleep_duration)

    asyncio.create_task(fetch_scheduler_loop())

@app.get("/")
def read_root():
    return {"status": "Backend is running"}
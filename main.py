import asyncio
import logging
from fastapi import FastAPI
from datetime import datetime, timedelta

from db import Base, engine
from config import IST_OFFSET
from utils import is_market_open
from processors.fetch_oc_snapshot import fetcher, closing_snapshot_check

TESTING = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

Base.metadata.create_all(bind=engine)
app = FastAPI(title="Options Dashboard API")

logger = logging.getLogger(__name__)


@app.on_event("startup")
async def start_fetcher():
    async def fetcher_loop():
        while True:
            now = datetime.utcnow() + IST_OFFSET

            if is_market_open(now, TESTING):
                asyncio.create_task(fetcher())

            # Sleep until the next exact minute
            next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            sleep_duration = max((next_minute - (datetime.utcnow() + IST_OFFSET)).total_seconds(), 0)
            await asyncio.sleep(sleep_duration)

    asyncio.create_task(fetcher_loop())
    asyncio.create_task(closing_snapshot_check())

@app.get("/")
def read_root():
    return {"status": "Backend is running"}
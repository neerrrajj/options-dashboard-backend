import asyncio
import logging
import os
from fastapi import FastAPI
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware

from db import Base, engine
from config import IST_TIMEZONE, OPERATIONAL
from utils import is_market_open
from api import gex, greeks, symbols, positional
from processors.fetch_oc_snapshot import fetcher, closing_snapshot_check
from queue_manager import init_queue
from processors.oc_processor import queue_consumer
from scheduler import start_scheduler

TESTING = False

# Get allowed origins from env or use defaults
FRONTEND_URL = os.getenv("FRONTEND_URL", "")
DEFAULT_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
# Add production frontend URL if set
if FRONTEND_URL:
    DEFAULT_ORIGINS.append(FRONTEND_URL)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

Base.metadata.create_all(bind=engine)
app = FastAPI(title="optionstrike API")

logger = logging.getLogger(__name__)


@app.on_event("startup")
async def start_services():
    # Initialize queue
    init_queue(maxsize=100)
    logger.info("[STARTUP] Queue initialized")
    
    # Start queue consumer
    asyncio.create_task(queue_consumer())
    logger.info("[STARTUP] Queue consumer started")
    
    # Start scheduler (retention jobs only)
    await start_scheduler()
    logger.info("[STARTUP] Scheduler started")
    
    # Start fetcher only if operational
    if OPERATIONAL:
        async def fetcher_loop():
            while True:
                now = datetime.now(IST_TIMEZONE)

                if is_market_open(now, TESTING):
                    asyncio.create_task(fetcher())

                # Sleep until the next exact minute
                next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                sleep_duration = max((next_minute - datetime.now(IST_TIMEZONE)).total_seconds(), 0)
                await asyncio.sleep(sleep_duration)

        asyncio.create_task(fetcher_loop())
        asyncio.create_task(closing_snapshot_check())
        logger.info("[STARTUP] Fetcher and closing snapshot check started (OPERATIONAL=true)")
    else:
        logger.info("[STARTUP] Fetcher disabled (OPERATIONAL=false) - serving historical data only")

app.add_middleware(
    CORSMiddleware,
    allow_origins=DEFAULT_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"status": "Backend is running"}

# app.include_router(meta.router)
app.include_router(gex.router)
app.include_router(greeks.router)
app.include_router(symbols.router)
app.include_router(positional.router)

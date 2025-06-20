import asyncio
import logging
from fastapi import FastAPI

from db import Base, engine
from fetcher.fetcher import fetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

Base.metadata.create_all(bind=engine)
app = FastAPI(title="Options Dashboard API")

@app.on_event("startup")
async def start_fetcher():
    async def fetch_loop():
        while True:
            await fetcher()
            await asyncio.sleep(30)

    asyncio.create_task(fetch_loop())

@app.get("/")
def read_root():
    return {"status": "Backend is running"}
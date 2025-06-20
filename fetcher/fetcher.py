import httpx
import asyncio
import time as timer
import logging
from sqlalchemy import and_
from datetime import datetime, time, timedelta

from db import SessionLocal
from models import OCSnapshot
from fetcher.snapshot_tasks import save_snapshot_task
from config import DHAN_API_URL, DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID, INSTRUMENTS

logger = logging.getLogger(__name__)

fetch_cycle_count = 0
IST_OFFSET = timedelta(hours=5, minutes=30)

headers = {
    "Content-Type": "application/json",
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID
}

def is_market_open():
    now_ist = datetime.utcnow() + IST_OFFSET
    return time(9, 15) <= now_ist.time() <= time(15, 30)

def is_closing_snapshot_needed(db, instrument, expiry):
    now_utc = datetime.utcnow()
    now_ist = now_utc + IST_OFFSET
    today_ist = now_ist.date()
    market_open_today_ist = datetime.combine(today_ist, time(9, 15))
    market_open_today_utc = market_open_today_ist - IST_OFFSET
    market_close_today_ist = datetime.combine(today_ist, time(15, 30))
    market_close_today_utc = market_close_today_ist - IST_OFFSET

    if now_utc > market_close_today_utc:
        closing_snapshot_exists = db.query(OCSnapshot).filter(
            and_(
                OCSnapshot.instrument == instrument["SECURITY_ID"],
                OCSnapshot.expiry == expiry,
                OCSnapshot.timestamp >= market_close_today_utc,
                OCSnapshot.timestamp <= now_utc
            )
        ).first()

        if closing_snapshot_exists:
            logger.info(f"Closing snapshot already exists for {instrument['SECURITY_ID']} today, skipping")
            return False
        else:
            logger.info(f"No closing snapshot for {instrument['SECURITY_ID']} today, fetching")
            return True

    if now_utc < market_open_today_utc:
        yesterday_ist = today_ist - timedelta(days=1)
        market_close_yesterday_ist = datetime.combine(yesterday_ist, time(15, 30))
        market_close_yesterday_utc = market_close_yesterday_ist - IST_OFFSET

        closing_snapshot_exists = db.query(OCSnapshot).filter(
            and_(
                OCSnapshot.instrument == instrument["SECURITY_ID"],
                OCSnapshot.expiry == expiry,
                OCSnapshot.timestamp >= market_close_yesterday_utc,
                OCSnapshot.timestamp <= now_utc
            )
        ).first()

        if closing_snapshot_exists:
            logger.info(f"Closing snapshot exists from yesterday for {instrument['SECURITY_ID']}, skipping")
            return False
        else:
            logger.info(f"No closing snapshot from yesterday for {instrument['SECURITY_ID']}, fetching")
            return True
    
    return False

def get_ordered_expiry_dates(expiries):
    """Get the expiry dates in ascending order"""
    if not expiries:
        return None
    today = datetime.now().date()
    expiry_dates = []
    for exp in expiries:
        exp_date = datetime.strptime(exp, "%Y-%m-%d").date() # YYYY-MM-DD
        if exp_date >= today:
            expiry_dates.append((exp_date, exp))

    if not expiry_dates:
        return expiries[0]

    expiry_dates.sort(key=lambda x: x[0])
    return expiry_dates

async def fetch_expiries(client, instrument):
    url = f"{DHAN_API_URL}/optionchain/expirylist"
    request_body = {
        "UnderlyingScrip": instrument["UNDERLYING_SYMBOL"],
        "UnderlyingSeg": instrument["UNDERLYING_SEGMENT"],
    }

    response = await client.post(url, json=request_body, headers=headers)
    response.raise_for_status()
    return response.json()["data"]

async def fetch_chain_for_expiry(client, instrument, expiry):
    url = f"{DHAN_API_URL}/optionchain"
    request_body = {
        "UnderlyingScrip": instrument["UNDERLYING_SYMBOL"],
        "UnderlyingSeg": instrument["UNDERLYING_SEGMENT"],
        "Expiry": expiry
    }

    response = await client.post(url, json=request_body, headers=headers)
    response.raise_for_status()
    return response.json()["data"]

async def fetch_oc_data(db, client, instrument, expiry):
    """Fetch option chain data for an instrument for an expiry"""
    logger.info(f"=== Fetching option chain data of {instrument['SECURITY_ID']} for {expiry} ===")

    try:
        oc_response = await fetch_chain_for_expiry(client, instrument, expiry)
        save_snapshot_task.delay(instrument, expiry, oc_response)

    except Exception as e:
        logger.error(f"Error fetching option chain data of {instrument['SECURITY_ID']} for {expiry}: {e}")

async def fetcher():
    start = timer.time()
    global fetch_cycle_count

    async with httpx.AsyncClient() as client:
        db = SessionLocal()
        try:
            logger.info(f"=== Fetch Cycle {fetch_cycle_count + 1} ===")

            # Fetching current expiries
            logger.info("Fetching for current expiry...")
            current_start = timer.time()
            current_count = 0

            for instrument in INSTRUMENTS:
                expiries = await fetch_expiries(client, instrument)
                current_expiry = get_ordered_expiry_dates(expiries)[0][1]

                if is_market_open() or is_closing_snapshot_needed(db, instrument, current_expiry):
                    instrument_start = timer.time()
                    await fetch_oc_data(db, client, instrument, current_expiry)
                    instrument_end = timer.time()
                    logger.info(f"{instrument['SECURITY_ID']} current: {(instrument_end - instrument_start):.2f}s")
                    await asyncio.sleep(3)
                    current_count += 1

            current_end = timer.time()
            logger.info(f"Current expiry total ({current_count} instruments): {(current_end - current_start):.2f}s")

            # # Fetching other expiries
            # logger.info("Fetching for other expiries...")
            # if fetch_cycle_count % 6 == 0:
            #     other_start = timer.time()
            #     other_count = 0

            #     for instrument in INSTRUMENTS:
            #         expiries = await fetch_expiries(client, instrument)
            #         current_expiry = get_ordered_expiry_dates(expiries)[0][1]
            #         limited_expiries = expiries[:7]
            #         other_expiries = [exp for exp in limited_expiries if exp != current_expiry]

            #         for expiry in other_expiries:
            #             if is_market_open() or is_closing_snapshot_needed(db, instrument, expiry):
            #                 await fetch_oc_data(db, client, instrument, expiry)
            #                 await asyncio.sleep(3)
            #                 other_count += 1

            #     other_end = timer.time()
            #     logger.info(f"Other expiries total ({other_count} instruments): {(other_end - other_start):.2f}s")
            # else:
            #     logger.info("Skipping other expiries this cycle")

            fetch_cycle_count += 1

        except Exception as e:
            logger.error(f"Error in fetch loop: {e}")
        finally:
            db.close()

    total_end = timer.time()
    logger.info(f"Total fetch cycle time: {(total_end - start):.2f}s")
    logger.info("-" * 50)

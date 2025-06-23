import httpx
import logging
import asyncio
import time as timer
from datetime import datetime, time, timedelta

from db import SessionLocal
from tasks.save_oc_snapshot import save_oc_snapshot_task
from models import OCMinuteSnapshot, HistoricalOCSnapshot
from utils import get_last_trading_day, is_trading_day, is_pre_market_hours
from config import DHAN_API_URL, DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID, INSTRUMENTS, IST_OFFSET

logger = logging.getLogger(__name__)

fetch_cycle_count = 1
headers = {
    "Content-Type": "application/json",
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID
}

async def closing_snapshot_check():
    """Check if last trading day's closing snapshot exists in the correct table. If missing, fetch and store it once."""
    db = SessionLocal()

    try:
        now_ist = datetime.utcnow() + IST_OFFSET
        today_ist = now_ist.date()

        if is_trading_day(today_ist):
            if is_pre_market_hours(now_ist):
                target_trading_day = get_last_trading_day(today_ist - timedelta(days=1))
                check_time_ist = datetime.combine(target_trading_day, time(15, 25))
                table = HistoricalOCSnapshot
            elif time(9, 0) <= now_ist.time() <= time(15, 31):
                logger.info("[CLOSE CHECK] Market is currently open, skipping closing snapshot check")
                return
            else:
                target_trading_day = today_ist
                check_time_ist = datetime.combine(today_ist, time(15, 29))
                table = OCMinuteSnapshot
        else:
            target_trading_day = get_last_trading_day(today_ist - timedelta(days=1))
            check_time_ist = datetime.combine(target_trading_day, time(15, 25))
            table = HistoricalOCSnapshot

        closing_snapshot_time = datetime.combine(target_trading_day, time(15, 29))

        logger.info(f"[CLOSE CHECK] Checking for closing snapshot of {target_trading_day} at {check_time_ist} in {table.__tablename__}")
        async with httpx.AsyncClient() as client:
            for instrument in INSTRUMENTS:
                instrument_id = instrument["SECURITY_ID"]
                expiries = await fetch_expiries(client, instrument)

                top_expiries = get_top_n_expiries(instrument, expiries)
                if not top_expiries:
                    logger.warning(f"[CLOSE CHECK] No valid expiries found for {instrument_id}")
                    continue

                for expiry_date, expiry in top_expiries:
                    exists = db.query(table).filter(
                        table.instrument == instrument_id,
                        table.expiry == expiry,
                        table.ist_minute == check_time_ist
                    ).first()
                    if exists:
                        logger.info(f"[CLOSE CHECK] {table.__tablename__} has {instrument_id} ({expiry}) snapshot at {check_time_ist}")
                        continue

                    logger.warning(f"[CLOSE CHECK] Missing {instrument_id} ({expiry}) snapshot at {check_time_ist} in {table.__tablename__}. Fetching...")
                    try:
                        await fetch_oc_data(db, client, instrument, expiry, closing_snapshot_time=closing_snapshot_time)
                    except Exception as e:
                        logger.error(f"[CLOSE CHECK] Error fetching closing snapshot for {instrument_id} ({expiry}): {e}")

                    await asyncio.sleep(3)

    except Exception as e:
        logger.error(f"[CLOSE CHECK] Unexpected error: {e}")
    finally:
        db.close()

def get_top_n_expiries(instrument, expiries, expiry_limit=None):
    """Get the top N expiry dates in ascending order"""
    if not expiries:
        return []
    if expiry_limit is None:
        expiry_limit = instrument.get("EXPIRIES", 7)
    today = datetime.now().date()
    expiry_dates = []

    for exp in expiries:
        exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
        if exp_date >= today:
            expiry_dates.append((exp_date, exp))
    if not expiry_dates:
        return []

    expiry_dates.sort(key=lambda x: x[0])
    return expiry_dates[:expiry_limit]

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

async def fetch_oc_data(db, client, instrument, expiry, closing_snapshot_time=None):
    """Fetch option chain data for an instrument for an expiry"""
    logger.info(f"=== Fetching option chain data of {instrument['SECURITY_ID']} for {expiry} ===")

    try:
        oc_response = await fetch_chain_for_expiry(client, instrument, expiry)
        save_oc_snapshot_task.delay(instrument, expiry, oc_response, closing_snapshot_time)

    except Exception as e:
        logger.error(f"Error fetching option chain data of {instrument['SECURITY_ID']} for {expiry}: {e}")

async def fetcher():
    start = timer.time()
    global fetch_cycle_count

    async with httpx.AsyncClient() as client:
        db = SessionLocal()
        try:
            logger.info(f"=== Fetch Cycle {fetch_cycle_count} ===")

            # Fetching for current expiries
            logger.info("Fetching for current expiry...")
            current_start = timer.time()
            current_count = 0

            for instrument in INSTRUMENTS:
                expiries = await fetch_expiries(client, instrument)
                top_expiries = get_top_n_expiries(instrument, expiries)
                if not top_expiries:
                    logger.warning(f"No valid expiries found for {instrument['SECURITY_ID']}")
                    continue
                current_expiry = top_expiries[0][1]

                instrument_start = timer.time()
                await fetch_oc_data(db, client, instrument, current_expiry)
                instrument_end = timer.time()
                logger.info(f"{instrument['SECURITY_ID']} current: {(instrument_end - instrument_start):.2f}s")
                await asyncio.sleep(3)
                current_count += 1

            current_end = timer.time()
            logger.info(f"Current expiry total ({current_count} instruments): {(current_end - current_start):.2f}s")

            # Fetching for other expiries
            logger.info("Fetching for other expiries...")
            other_start = timer.time()
            other_count = 0

            for instrument in INSTRUMENTS:
                expiries = await fetch_expiries(client, instrument)
                top_expiries = get_top_n_expiries(instrument, expiries)
                if not top_expiries:
                    logger.warning(f"No valid expiries found for {instrument['SECURITY_ID']}")
                    continue
                current_expiry = top_expiries[0][1]
                other_expiries = [expiry for expiry_date, expiry in top_expiries[1:]]

                for expiry in other_expiries:
                    await fetch_oc_data(db, client, instrument, expiry)
                    await asyncio.sleep(3)
                    other_count += 1

            other_end = timer.time()
            logger.info(f"Other expiries total ({other_count} instruments): {(other_end - other_start):.2f}s")

            fetch_cycle_count += 1

        except Exception as e:
            logger.error(f"Error in fetch loop: {e}")
        finally:
            db.close()

    total_end = timer.time()
    logger.info(f"Total fetch cycle time: {(total_end - start):.2f}s")
    logger.info("-" * 50)

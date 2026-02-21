"""Option chain data processor.

This module handles processing of fetched option chain data,
including batch UPSERT operations and summary calculations.
Replaces the Celery-based task system.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.dialects.postgresql import insert

from db import SessionLocal
from models import OCMinuteSnapshot, OCSummary
from queue_manager import OCDataItem, get_queue
from config import INSTRUMENTS, IST_TIMEZONE

logger = logging.getLogger(__name__)


def compute_gex_metrics(row_data: dict) -> dict:
    """Compute GEX metrics for a strike row.
    
    Args:
        row_data: Dictionary containing gamma and OI values
        
    Returns:
        Dictionary with computed GEX fields
    """
    call_gamma = row_data.get("call_gamma") or 0.0
    call_oi = row_data.get("call_oi") or 0
    put_gamma = row_data.get("put_gamma") or 0.0
    put_oi = row_data.get("put_oi") or 0
    
    call_gex = call_gamma * call_oi
    put_gex = put_gamma * put_oi
    net_gex = call_gex - put_gex
    abs_gex = abs(call_gex) + abs(put_gex)
    
    return {
        "call_gex": call_gex,
        "put_gex": put_gex,
        "net_gex": net_gex,
        "abs_gex": abs_gex,
    }


def prepare_strike_data(
    instrument: dict,
    expiry: str,
    oc: dict,
    underlying_price: float,
    ist_minute: datetime,
    snapshot_time: datetime
) -> list[dict]:
    """Prepare strike data rows for batch insert.
    
    Args:
        instrument: Instrument configuration dict
        expiry: Expiry date string
        oc: Option chain data from API
        underlying_price: Current underlying price
        ist_minute: IST minute timestamp
        snapshot_time: UTC snapshot timestamp
        
    Returns:
        List of row dictionaries ready for insert
    """
    instrument_id = instrument["SECURITY_ID"]
    strike_range = instrument["STRIKE_RANGE"]
    
    # Calculate ATM bounds
    atm = round(underlying_price / strike_range) * strike_range
    lower_bound = atm - 40 * strike_range
    upper_bound = atm + 40 * strike_range
    
    rows = []
    for strike_str, chain in oc.items():
        strike = float(strike_str)
        if not (lower_bound <= strike <= upper_bound):
            continue
        
        ce = chain.get("ce", {})
        pe = chain.get("pe", {})
        
        row = {
            "timestamp": snapshot_time,
            "ist_minute": ist_minute,
            "instrument": instrument_id,
            "expiry": expiry,
            "strike": strike,
            "underlying_price": underlying_price,
            "call_delta": ce.get("greeks", {}).get("delta"),
            "call_theta": ce.get("greeks", {}).get("theta"),
            "call_gamma": ce.get("greeks", {}).get("gamma"),
            "call_vega": ce.get("greeks", {}).get("vega"),
            "call_iv": ce.get("implied_volatility"),
            "call_oi": ce.get("oi"),
            "call_volume": ce.get("volume"),
            "call_last_price": ce.get("last_price"),
            "put_delta": pe.get("greeks", {}).get("delta"),
            "put_theta": pe.get("greeks", {}).get("theta"),
            "put_gamma": pe.get("greeks", {}).get("gamma"),
            "put_vega": pe.get("greeks", {}).get("vega"),
            "put_iv": pe.get("implied_volatility"),
            "put_oi": pe.get("oi"),
            "put_volume": pe.get("volume"),
            "put_last_price": pe.get("last_price"),
        }
        
        # Compute GEX metrics
        gex_metrics = compute_gex_metrics(row)
        row.update(gex_metrics)
        rows.append(row)
    
    return rows


def calculate_summary(
    rows: list[dict],
    instrument_id: str,
    expiry: str,
    underlying_price: float,
    ist_minute: datetime
) -> dict:
    """Calculate summary metrics from strike data.
    
    Args:
        rows: List of strike data rows
        instrument_id: Instrument identifier
        expiry: Expiry date
        underlying_price: Current underlying price
        ist_minute: IST minute timestamp
        
    Returns:
        Dictionary of summary metrics
    """
    if not rows:
        return {}
    
    # Find strike range for OTM determination
    instrument = next(
        (i for i in INSTRUMENTS if i["SECURITY_ID"] == instrument_id),
        None
    )
    if instrument is None:
        raise ValueError(f"Unknown instrument: {instrument_id}")
    
    strike_range = instrument["STRIKE_RANGE"]
    atm = round(underlying_price / strike_range) * strike_range
    
    # Calculate totals
    total_net_gex = sum(r["net_gex"] or 0 for r in rows)
    
    # Gamma flip level: strike where cumulative net GEX crosses zero
    rows_sorted = sorted(rows, key=lambda r: r["strike"])
    cum_net_gex = 0
    gamma_flip_level = None
    for row in rows_sorted:
        cum_net_gex += row["net_gex"] or 0
        if cum_net_gex >= 0:
            gamma_flip_level = row["strike"]
            break
    
    # OTM Greeks
    otm_call_vega = sum(r["call_vega"] or 0 for r in rows if r["strike"] >= atm)
    otm_put_vega = sum(r["put_vega"] or 0 for r in rows if r["strike"] <= atm)
    otm_call_theta = sum(r["call_theta"] or 0 for r in rows if r["strike"] >= atm)
    otm_put_theta = sum(r["put_theta"] or 0 for r in rows if r["strike"] <= atm)
    otm_call_delta = sum(r["call_delta"] or 0 for r in rows if r["strike"] >= atm)
    otm_put_delta = sum(r["put_delta"] or 0 for r in rows if r["strike"] <= atm)
    
    return {
        "timestamp": datetime.now(IST_TIMEZONE),
        "ist_minute": ist_minute,
        "instrument": instrument_id,
        "expiry": expiry,
        "underlying_price": underlying_price,
        "total_net_gex": total_net_gex,
        "gamma_flip_level": gamma_flip_level,
        "otm_call_vega": otm_call_vega,
        "otm_put_vega": otm_put_vega,
        "otm_call_theta": otm_call_theta,
        "otm_put_theta": otm_put_theta,
        "otm_call_delta": otm_call_delta,
        "otm_put_delta": otm_put_delta,
    }


def upsert_strikes(db, rows: list[dict]) -> int:
    """Upsert strike data rows using PostgreSQL ON CONFLICT.
    
    Args:
        db: Database session
        rows: List of row dictionaries
        
    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0
    
    # Build insert statement with on_conflict_do_update
    stmt = insert(OCMinuteSnapshot).values(rows)
    
    # Update all columns on conflict except primary key and unique constraint columns
    update_dict = {
        c.name: stmt.excluded[c.name]
        for c in OCMinuteSnapshot.__table__.columns
        if c.name not in ('id', 'instrument', 'expiry', 'ist_minute', 'strike')
    }
    
    stmt = stmt.on_conflict_do_update(
        index_elements=['instrument', 'expiry', 'ist_minute', 'strike'],
        set_=update_dict
    )
    
    result = db.execute(stmt)
    return result.rowcount


def upsert_summary(db, summary_data: dict) -> None:
    """Upsert summary data using PostgreSQL ON CONFLICT.
    
    Args:
        db: Database session
        summary_data: Dictionary of summary metrics
    """
    if not summary_data:
        return
    
    stmt = insert(OCSummary).values([summary_data])
    
    # Update all columns on conflict except primary key and unique constraint columns
    update_dict = {
        c.name: stmt.excluded[c.name]
        for c in OCSummary.__table__.columns
        if c.name not in ('id', 'instrument', 'expiry', 'ist_minute')
    }
    
    stmt = stmt.on_conflict_do_update(
        index_elements=['instrument', 'expiry', 'ist_minute'],
        set_=update_dict
    )
    
    db.execute(stmt)


def process_oc_data(item: OCDataItem) -> None:
    """Process a single OC data item.
    
    This is the main entry point that replaces the Celery task.
    It performs all database operations in a single transaction.
    
    Args:
        item: The data item to process
    """
    db = SessionLocal()
    
    try:
        # Extract data
        instrument = item.instrument
        expiry = item.expiry
        oc_response = item.oc_response
        closing_snapshot_time = item.closing_snapshot_time
        
        oc = oc_response.get("oc", {})
        underlying_price = oc_response.get("last_price")
        
        if not oc or underlying_price is None:
            logger.warning(f"[PROCESS] Invalid oc_response for {instrument['SECURITY_ID']} {expiry}")
            return
        
        # Determine timestamp
        snapshot_time = datetime.now(IST_TIMEZONE).replace(microsecond=0)
        if closing_snapshot_time:
            ist_minute = closing_snapshot_time
            # Ensure timezone-aware
            if ist_minute.tzinfo is None:
                ist_minute = ist_minute.replace(tzinfo=IST_TIMEZONE)
        else:
            ist_minute = snapshot_time.replace(second=0, microsecond=0)
        
        logger.info(f"[PROCESS] Processing {instrument['SECURITY_ID']} ({expiry}) at IST {ist_minute}")
        
        # Prepare strike data
        strike_rows = prepare_strike_data(
            instrument, expiry, oc, underlying_price, ist_minute, snapshot_time
        )
        
        if not strike_rows:
            logger.warning(f"[PROCESS] No strike data for {instrument['SECURITY_ID']} {expiry}")
            return
        
        # Upsert strikes
        upserted_count = upsert_strikes(db, strike_rows)
        logger.info(f"[PROCESS] Upserted {upserted_count} strikes for {instrument['SECURITY_ID']} ({expiry})")
        
        # Calculate and upsert summary
        summary_data = calculate_summary(
            strike_rows,
            instrument["SECURITY_ID"],
            expiry,
            underlying_price,
            ist_minute
        )
        upsert_summary(db, summary_data)
        logger.info(f"[PROCESS] Upserted summary for {instrument['SECURITY_ID']} ({expiry})")
        
        # Commit transaction
        db.commit()
        logger.info(f"[PROCESS] Completed {instrument['SECURITY_ID']} ({expiry}) at IST {ist_minute}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"[PROCESS] Error processing {instrument.get('SECURITY_ID', 'unknown')}: {e}")
        raise
    finally:
        db.close()


async def queue_consumer() -> None:
    """Main consumer loop that processes items from the queue.
    
    This runs continuously as an asyncio task.
    """
    queue = get_queue()
    logger.info("[CONSUMER] Queue consumer started")
    
    while True:
        try:
            # Get item from queue (blocks until available)
            item = await queue.get()
            
            try:
                # Process the item
                process_oc_data(item)
            except Exception as e:
                logger.error(f"[CONSUMER] Error processing item: {e}")
                # Continue processing other items - don't crash the consumer
            finally:
                # Mark task as done
                queue.task_done()
                
        except Exception as e:
            logger.error(f"[CONSUMER] Fatal error in consumer loop: {e}")
            # Sleep briefly before retrying to avoid tight error loops
            await asyncio.sleep(1)


# Import asyncio here to avoid circular import issues
import asyncio

"""Data retention and rollup processor.

Handles rolling up 1-minute data to 5-minute intervals for data older than 30 days.
Runs nightly to maintain the rolling window of high-granularity data.
"""

import logging
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert

from db import SessionLocal
from models import (
    OCMinuteSnapshot, OCSummary,
    HistoricalOCSnapshot, HistoricalOCSummary
)
from utils import is_trading_day
from config import IST_TIMEZONE

logger = logging.getLogger(__name__)


def get_rollup_target_date(today: date = None) -> date:
    """Calculate which date should be rolled up.
    
    We keep 30 days of 1-minute data (today + 29 previous days).
    So we rollup data from 31 days ago.
    
    Args:
        today: Optional date to use as reference (defaults to today)
        
    Returns:
        Date that should be rolled up
    """
    if today is None:
        today = datetime.now(IST_TIMEZONE).date()
    
    # Rollup the date that is 31 days old
    target_date = today - timedelta(days=31)
    return target_date


def rollup_minute_snapshots(db, target_date: date) -> int:
    """Rollup 1-minute snapshots to 5-minute intervals for a specific date.
    
    Args:
        db: Database session
        target_date: Date to rollup
        
    Returns:
        Number of rows inserted into historical table
    """
    day_start = datetime.combine(target_date, time.min)
    day_end = datetime.combine(target_date, time.max)
    
    logger.info(f"[RETENTION] Rolling up minute snapshots for {target_date}")
    
    # Get all unique (instrument, expiry, strike) combinations for this date
    combinations = db.query(
        OCMinuteSnapshot.instrument,
        OCMinuteSnapshot.expiry,
        OCMinuteSnapshot.strike
    ).filter(
        OCMinuteSnapshot.ist_minute >= day_start,
        OCMinuteSnapshot.ist_minute <= day_end
    ).distinct().all()
    
    if not combinations:
        logger.info(f"[RETENTION] No minute snapshot data found for {target_date}")
        return 0
    
    logger.info(f"[RETENTION] Found {len(combinations)} (instrument, expiry, strike) combinations")
    
    total_inserted = 0
    
    for instrument, expiry, strike in combinations:
        # Get all rows for this combination
        rows = db.query(OCMinuteSnapshot).filter(
            OCMinuteSnapshot.instrument == instrument,
            OCMinuteSnapshot.expiry == expiry,
            OCMinuteSnapshot.strike == strike,
            OCMinuteSnapshot.ist_minute >= day_start,
            OCMinuteSnapshot.ist_minute <= day_end
        ).order_by(OCMinuteSnapshot.ist_minute).all()
        
        if not rows:
            continue
        
        # Group into 5-minute buckets
        buckets = {}
        for row in rows:
            # Round down to nearest 5-minute interval
            bucket_time = row.ist_minute.replace(
                minute=(row.ist_minute.minute // 5) * 5,
                second=0,
                microsecond=0
            )
            
            # Keep the last row in each bucket (most recent within the 5-min window)
            buckets[bucket_time] = row
        
        # Prepare rows for insertion
        hist_rows = []
        for bucket_time, row in buckets.items():
            hist_row = {
                'timestamp': row.timestamp,
                'ist_minute': bucket_time,
                'instrument': row.instrument,
                'expiry': row.expiry,
                'underlying_price': row.underlying_price,
                'strike': row.strike,
                'call_delta': row.call_delta,
                'call_theta': row.call_theta,
                'call_gamma': row.call_gamma,
                'call_vega': row.call_vega,
                'call_iv': row.call_iv,
                'call_oi': row.call_oi,
                'call_volume': row.call_volume,
                'call_last_price': row.call_last_price,
                'put_delta': row.put_delta,
                'put_theta': row.put_theta,
                'put_gamma': row.put_gamma,
                'put_vega': row.put_vega,
                'put_iv': row.put_iv,
                'put_oi': row.put_oi,
                'put_volume': row.put_volume,
                'put_last_price': row.put_last_price,
                'call_gex': row.call_gex,
                'put_gex': row.put_gex,
                'net_gex': row.net_gex,
                'abs_gex': row.abs_gex,
            }
            hist_rows.append(hist_row)
        
        # Batch upsert historical rows
        if hist_rows:
            stmt = insert(HistoricalOCSnapshot).values(hist_rows)
            update_dict = {
                c.name: stmt.excluded[c.name]
                for c in HistoricalOCSnapshot.__table__.columns
                if c.name not in ('id', 'instrument', 'expiry', 'ist_minute', 'strike')
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=['instrument', 'expiry', 'ist_minute', 'strike'],
                set_=update_dict
            )
            result = db.execute(stmt)
            total_inserted += result.rowcount
    
    logger.info(f"[RETENTION] Inserted/updated {total_inserted} historical snapshot rows")
    return total_inserted


def rollup_summary(db, target_date: date) -> int:
    """Rollup 1-minute summary to 5-minute intervals for a specific date.
    
    Args:
        db: Database session
        target_date: Date to rollup
        
    Returns:
        Number of rows inserted into historical table
    """
    day_start = datetime.combine(target_date, time.min)
    day_end = datetime.combine(target_date, time.max)
    
    logger.info(f"[RETENTION] Rolling up summary for {target_date}")
    
    # Get all unique (instrument, expiry) combinations for this date
    combinations = db.query(
        OCSummary.instrument,
        OCSummary.expiry
    ).filter(
        OCSummary.ist_minute >= day_start,
        OCSummary.ist_minute <= day_end
    ).distinct().all()
    
    if not combinations:
        logger.info(f"[RETENTION] No summary data found for {target_date}")
        return 0
    
    logger.info(f"[RETENTION] Found {len(combinations)} (instrument, expiry) combinations")
    
    total_inserted = 0
    
    for instrument, expiry in combinations:
        # Get all rows for this combination
        rows = db.query(OCSummary).filter(
            OCSummary.instrument == instrument,
            OCSummary.expiry == expiry,
            OCSummary.ist_minute >= day_start,
            OCSummary.ist_minute <= day_end
        ).order_by(OCSummary.ist_minute).all()
        
        if not rows:
            continue
        
        # Group into 5-minute buckets
        buckets = {}
        for row in rows:
            # Round down to nearest 5-minute interval
            bucket_time = row.ist_minute.replace(
                minute=(row.ist_minute.minute // 5) * 5,
                second=0,
                microsecond=0
            )
            
            # Keep the last row in each bucket
            buckets[bucket_time] = row
        
        # Prepare rows for insertion
        hist_rows = []
        for bucket_time, row in buckets.items():
            hist_row = {
                'timestamp': row.timestamp,
                'ist_minute': bucket_time,
                'instrument': row.instrument,
                'expiry': row.expiry,
                'underlying_price': row.underlying_price,
                'total_net_gex': row.total_net_gex,
                'gamma_flip_level': row.gamma_flip_level,
                'otm_call_vega': row.otm_call_vega,
                'otm_put_vega': row.otm_put_vega,
                'otm_call_theta': row.otm_call_theta,
                'otm_put_theta': row.otm_put_theta,
                'otm_call_delta': row.otm_call_delta,
                'otm_put_delta': row.otm_put_delta,
            }
            hist_rows.append(hist_row)
        
        # Batch upsert historical rows
        if hist_rows:
            stmt = insert(HistoricalOCSummary).values(hist_rows)
            update_dict = {
                c.name: stmt.excluded[c.name]
                for c in HistoricalOCSummary.__table__.columns
                if c.name not in ('id', 'instrument', 'expiry', 'ist_minute')
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=['instrument', 'expiry', 'ist_minute'],
                set_=update_dict
            )
            result = db.execute(stmt)
            total_inserted += result.rowcount
    
    logger.info(f"[RETENTION] Inserted/updated {total_inserted} historical summary rows")
    return total_inserted


def delete_rolled_up_data(db, target_date: date) -> tuple[int, int]:
    """Delete 1-minute data that has been rolled up.
    
    Args:
        db: Database session
        target_date: Date to delete
        
    Returns:
        Tuple of (deleted_snapshots, deleted_summaries)
    """
    day_start = datetime.combine(target_date, time.min)
    day_end = datetime.combine(target_date, time.max)
    
    logger.info(f"[RETENTION] Deleting 1-minute data for {target_date}")
    
    # Delete minute snapshots
    snapshot_count = db.query(OCMinuteSnapshot).filter(
        OCMinuteSnapshot.ist_minute >= day_start,
        OCMinuteSnapshot.ist_minute <= day_end
    ).delete(synchronize_session=False)
    
    # Delete summaries
    summary_count = db.query(OCSummary).filter(
        OCSummary.ist_minute >= day_start,
        OCSummary.ist_minute <= day_end
    ).delete(synchronize_session=False)
    
    logger.info(f"[RETENTION] Deleted {snapshot_count} snapshots and {summary_count} summaries")
    return snapshot_count, summary_count


def rollup_day(target_date: date = None) -> dict:
    """Rollup a specific day's data to 5-minute intervals.
    
    This is the main entry point for the retention job.
    
    Args:
        target_date: Date to rollup (defaults to 31 days ago)
        
    Returns:
        Dictionary with statistics about the rollup operation
    """
    if target_date is None:
        target_date = get_rollup_target_date()
    
    logger.info(f"[RETENTION] Starting rollup for {target_date}")
    
    # Skip if not a trading day
    if not is_trading_day(target_date):
        logger.info(f"[RETENTION] {target_date} is not a trading day, skipping")
        return {
            "date": target_date.isoformat(),
            "status": "skipped",
            "reason": "not_trading_day"
        }
    
    db = SessionLocal()
    
    try:
        # Rollup snapshots
        snapshot_count = rollup_minute_snapshots(db, target_date)
        
        # Rollup summary
        summary_count = rollup_summary(db, target_date)
        
        # Delete original 1-minute data
        deleted_snapshots, deleted_summaries = delete_rolled_up_data(db, target_date)
        
        # Commit transaction
        db.commit()
        
        result = {
            "date": target_date.isoformat(),
            "status": "success",
            "snapshots_rolled_up": snapshot_count,
            "summaries_rolled_up": summary_count,
            "snapshots_deleted": deleted_snapshots,
            "summaries_deleted": deleted_summaries,
        }
        
        logger.info(f"[RETENTION] Rollup complete for {target_date}: {result}")
        return result
        
    except Exception as e:
        db.rollback()
        logger.error(f"[RETENTION] Error rolling up {target_date}: {e}")
        return {
            "date": target_date.isoformat(),
            "status": "error",
            "error": str(e)
        }
    finally:
        db.close()


def run_retention_job() -> dict:
    """Run the nightly retention job.
    
    This is the entry point that should be called from the scheduler.
    
    Returns:
        Dictionary with job results
    """
    logger.info("[RETENTION] Starting nightly retention job")
    
    target_date = get_rollup_target_date()
    result = rollup_day(target_date)
    
    logger.info("[RETENTION] Nightly retention job complete")
    return result

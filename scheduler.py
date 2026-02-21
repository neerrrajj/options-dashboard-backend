"""Scheduler for background jobs.

Runs retention and other periodic tasks using asyncio.
Replaces Celery Beat functionality.
"""

import asyncio
import logging
from datetime import datetime, time, timedelta

from processors.retention import run_retention_job
from utils import is_trading_day

logger = logging.getLogger(__name__)


async def sleep_until(target_time: time) -> None:
    """Sleep until a specific time today (or tomorrow if already past).
    
    Args:
        target_time: Time of day to wake up at
    """
    now = datetime.utcnow()
    target = datetime.combine(now.date(), target_time)
    
    # If target time has already passed today, schedule for tomorrow
    if target <= now:
        target += timedelta(days=1)
    
    sleep_seconds = (target - now).total_seconds()
    logger.info(f"[SCHEDULER] Sleeping for {sleep_seconds:.0f} seconds until {target}")
    await asyncio.sleep(sleep_seconds)


async def retention_job_runner() -> None:
    """Run retention job every night at 11:45 PM UTC.
    
    Runs continuously, executing the job daily.
    """
    # Run at 11:45 PM UTC (5:15 AM IST next day, after market close)
    RUN_TIME = time(23, 45)
    
    logger.info(f"[SCHEDULER] Retention job scheduler started, will run daily at {RUN_TIME} UTC")
    
    while True:
        try:
            # Sleep until next run time
            await sleep_until(RUN_TIME)
            
            # Run the retention job
            logger.info("[SCHEDULER] Executing retention job")
            result = run_retention_job()
            
            if result.get("status") == "success":
                logger.info(f"[SCHEDULER] Retention job completed successfully")
            elif result.get("status") == "skipped":
                logger.info(f"[SCHEDULER] Retention job skipped: {result.get('reason')}")
            else:
                logger.error(f"[SCHEDULER] Retention job failed: {result.get('error')}")
            
            # Small delay to prevent immediate re-run if execution was very fast
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error in retention job runner: {e}")
            # Sleep for a bit before retrying to avoid tight error loops
            await asyncio.sleep(300)


async def start_scheduler() -> None:
    """Start all background schedulers.
    
    This should be called from main.py on startup.
    """
    logger.info("[SCHEDULER] Starting background schedulers")
    
    # Start retention job scheduler
    asyncio.create_task(retention_job_runner())
    
    logger.info("[SCHEDULER] All schedulers started")


def manual_run_retention(date_str: str = None) -> dict:
    """Manually trigger retention job for a specific date.
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD). If None, uses 31 days ago.
        
    Returns:
        Result dictionary from rollup operation
    """
    from processors.retention import rollup_day
    from datetime import date
    
    if date_str:
        target_date = date.fromisoformat(date_str)
    else:
        target_date = None
    
    logger.info(f"[SCHEDULER] Manual retention job triggered for {date_str or 'default'}")
    return rollup_day(target_date)

import logging
from datetime import datetime, time

from models import OCMinuteSnapshot, OCSummary

logger = logging.getLogger(__name__)

def cleanup_intraday_data(db, instrument, ist_date):
    """
    Deletes all rows from:
    - OCSnapshot (based on UTC snapshot_time)
    - OCMinuteSnapshot (based on ist_minute)
    - OCSummary (based on ist_minute)
    for a given instrument and IST date.
    """
    # For OCMinuteSnapshot + OCSummary (IST-based)
    day_start_ist = datetime.combine(ist_date, time.min)
    day_end_ist = datetime.combine(ist_date, time.max)

    # # For OCSnapshot (UTC-based)
    # day_start_utc = day_start_ist - timedelta(hours=5, minutes=30)
    # day_end_utc = day_end_ist - timedelta(hours=5, minutes=30)

    # # Delete OCSnapshot
    # snapshot_count = db.query(OCSnapshot).filter(
    #     OCSnapshot.instrument == instrument,
    #     OCSnapshot.snapshot_time >= day_start_utc,
    #     OCSnapshot.snapshot_time <= day_end_utc
    # ).delete(synchronize_session=False)

    # Delete OCMinuteSnapshot
    minute_count = db.query(OCMinuteSnapshot).filter(
        OCMinuteSnapshot.instrument == instrument,
        OCMinuteSnapshot.ist_minute >= day_start_ist,
        OCMinuteSnapshot.ist_minute <= day_end_ist
    ).delete(synchronize_session=False)

    # Delete OCSummary
    summary_count = db.query(OCSummary).filter(
        OCSummary.instrument == instrument,
        OCSummary.ist_minute >= day_start_ist,
        OCSummary.ist_minute <= day_end_ist
    ).delete(synchronize_session=False)

    logger.info(
        f"[DAILY CLEANUP] Deleted no raw snapshot rows (UTC), {minute_count} 1-min, and {summary_count} summary rows for {instrument} on IST {ist_date}"
    )

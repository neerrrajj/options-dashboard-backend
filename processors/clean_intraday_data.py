import logging
from datetime import datetime, time

from models import OCMinuteSnapshot, OCSummary

logger = logging.getLogger(__name__)

def cleanup_intraday_data(db, instrument, ist_date):
    "Deletes all rows from OCMinuteSnapshot and OCSummary (based on ist_minute) for a given instrument and IST date."

    day_start_ist = datetime.combine(ist_date, time.min)
    day_end_ist = datetime.combine(ist_date, time.max)

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

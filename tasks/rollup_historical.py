import logging
from sqlalchemy import func
from datetime import datetime, time

from db import SessionLocal
from celery_config import celery_app
from processors.clean_intraday_data import cleanup_intraday_data
from models import OCMinuteSnapshot, OCSummary, HistoricalOCSnapshot, HistoricalOCSummary

logger = logging.getLogger(__name__)

@celery_app.task
def rollup_historical_task():
    db = SessionLocal()

    try:
        # Get all (instrument, ist_date) pairs from minute + summary tables
        snap_keys = db.query(
            OCMinuteSnapshot.instrument,
            func.date(OCMinuteSnapshot.ist_minute)
        ).distinct().all()

        summary_keys = db.query(
            OCSummary.instrument,
            func.date(OCSummary.ist_minute)
        ).distinct().all()

        combined_keys = set(snap_keys) | set(summary_keys)

        for instrument, ist_date in combined_keys:
            day_start = datetime.combine(ist_date, time.min)
            day_end = datetime.combine(ist_date, time.max)

            # --- Minute Snapshot Rollup (current expiry only) ---
            current_expiry = db.query(func.min(OCMinuteSnapshot.expiry)).filter(
                OCMinuteSnapshot.instrument == instrument,
                OCMinuteSnapshot.ist_minute >= day_start,
                OCMinuteSnapshot.ist_minute <= day_end
            ).scalar()

            logger.info(f"[H-ROLLUP] Rolling up {instrument} ({current_expiry}) at IST {ist_date}")

            if current_expiry:
                minute_rows = db.query(OCMinuteSnapshot).filter(
                    OCMinuteSnapshot.instrument == instrument,
                    OCMinuteSnapshot.expiry == current_expiry,
                    OCMinuteSnapshot.ist_minute >= day_start,
                    OCMinuteSnapshot.ist_minute <= day_end
                ).all()

                snapshots_by_5min = {}
                for row in minute_rows:
                    bucket_time = row.ist_minute.replace(
                        minute=(row.ist_minute.minute // 5) * 5,
                        second=0,
                        microsecond=0
                    )
                    key = (row.instrument, row.expiry, row.strike, bucket_time)
                    if key not in snapshots_by_5min or row.ist_minute > snapshots_by_5min[key].ist_minute:
                        snapshots_by_5min[key] = row

                for (instrument, expiry, strike, bucket_time), row in snapshots_by_5min.items():
                    exists = db.query(HistoricalOCSnapshot).filter_by(
                        instrument=instrument,
                        expiry=expiry,
                        strike=strike,
                        ist_minute=bucket_time
                    ).first()
                    if not exists:
                        new_row_data = {c.name: getattr(row, c.name) for c in OCMinuteSnapshot.__table__.columns}
                        new_row_data['ist_minute'] = bucket_time
                        db.add(HistoricalOCSnapshot(**new_row_data))

            # --- Summary Rollup (all expiries) ---
            summary_rows = db.query(OCSummary).filter(
                OCSummary.instrument == instrument,
                OCSummary.ist_minute >= day_start,
                OCSummary.ist_minute <= day_end
            ).all()

            summary_by_5min = {}
            for row in summary_rows:
                bucket_time = row.ist_minute.replace(
                    minute=(row.ist_minute.minute // 5) * 5,
                    second=0,
                    microsecond=0
                )
                key = (row.instrument, row.expiry, bucket_time)
                if key not in summary_by_5min or row.ist_minute > summary_by_5min[key].ist_minute:
                    summary_by_5min[key] = row

            for (instrument, expiry, bucket_time), row in summary_by_5min.items():
                exists = db.query(HistoricalOCSummary).filter_by(
                    instrument=instrument,
                    expiry=expiry,
                    ist_minute=bucket_time
                ).first()
                if not exists:
                    new_row_data = {c.name: getattr(row, c.name) for c in OCSummary.__table__.columns}
                    new_row_data['ist_minute'] = bucket_time
                    db.add(HistoricalOCSummary(**new_row_data))

            # --- Cleanup ---
            cleanup_intraday_data(db, instrument, ist_date)

        db.commit()
        logger.info("[H-ROLLUP] Full historical rollup completed")
    except Exception as e:
        db.rollback()
        logger.error(f"[H-ROLLUP] Rollup failed: {e}")
    finally:
        db.close()

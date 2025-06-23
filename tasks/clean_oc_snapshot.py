import logging

from db import SessionLocal
from models import OCSnapshot
from celery_config import celery_app

logger = logging.getLogger(__name__)

@celery_app.task
def clean_oc_snapshot_task(instrument_id, expiry, snapshot_time):
    db = SessionLocal()

    try:
        logger.info(f"[CLEAN SNAPSHOT] Cleaning for {instrument_id} ({expiry}) at snapshot time (UTC) {snapshot_time}")

        snapshot_count = db.query(OCSnapshot).filter(
            OCSnapshot.instrument == instrument_id,
            OCSnapshot.snapshot_time == snapshot_time
        ).delete(synchronize_session=False)

        db.commit()

        logger.info(f"[CLEAN SNAPSHOT] Cleaned up {snapshot_count} rows at snapshot time (UTC) {snapshot_time}")
    except Exception as e:
        db.rollback()
        logger.error(f"[CLEAN SNAPSHOT] Failed cleaning for {instrument_id} ({expiry}) at snapshot time (UTC) {snapshot_time}: {e}")
    finally:
        db.close()
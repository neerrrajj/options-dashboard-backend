import logging
from datetime import datetime

from db import SessionLocal
from models import OCSnapshot
from celery_config import celery_app
from tasks.rollup_minute import rollup_minute_task

logger = logging.getLogger(__name__)

@celery_app.task
def save_oc_snapshot_task(instrument, expiry, oc_response, fetch_cycle_count):
    """Save raw snapshot and trigger rollup task"""
    db = SessionLocal()
    try:
        oc, underlying_price = oc_response["oc"], oc_response["last_price"]
        snapshot_time = datetime.utcnow().replace(microsecond=0)

        for strike_str, chain in oc.items():
            strike = float(strike_str)
            for opt_type in ["ce", "pe"]:
                opt = chain.get(opt_type)
                if not opt:
                    continue

                snapshot = OCSnapshot(
                    snapshot_time=snapshot_time,
                    instrument=instrument["SECURITY_ID"],
                    expiry=expiry,
                    underlying_price=underlying_price,
                    strike=strike,
                    option_type=opt_type.upper(),
                    delta=opt["greeks"]["delta"],
                    theta=opt["greeks"]["theta"],
                    gamma=opt["greeks"]["gamma"],
                    vega=opt["greeks"]["vega"],
                    iv=opt["implied_volatility"],
                    oi=opt["oi"],
                    volume=opt["volume"],
                    last_price=opt["last_price"],
                )
                db.add(snapshot)

        db.commit()
        logger.info(f"[SAVE SNAPSHOT] Saved for {instrument['SECURITY_ID']} ({expiry}). Snapshot time (UTC): {snapshot_time}")

        # Trigger rollup
        rollup_minute_task.delay(instrument["SECURITY_ID"], expiry, snapshot_time.isoformat())

    except Exception as e:
        logger.error(f"[SAVE SNAPSHOT] Error saving for {instrument['SECURITY_ID']} ({expiry}). Snapshot time (UTC): {snapshot_time}. {e}")
        db.rollback()
    finally:
        db.close()

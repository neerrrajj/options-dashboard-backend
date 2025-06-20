import logging
from datetime import datetime

from db import SessionLocal
from models import OCSnapshot
from celery_config import celery_app
from rollup.rollup_minute import perform_rollup_for_instrument

logger = logging.getLogger(__name__)

@celery_app.task
def save_snapshot_task(instrument, expiry, oc_response):
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
        logger.info(f"Saved snapshot for {instrument['SECURITY_ID']}")

        # Trigger rollup
        rollup_task.delay(instrument["SECURITY_ID"], expiry, snapshot_time.isoformat())

    except Exception as e:
        logger.error(f"Error saving snapshot: {e}")
        db.rollback()
    finally:
        db.close()

@celery_app.task
def rollup_task(instrument_id, expiry, snapshot_time=None):
    """Deduplicated 1-min rollup task"""
    if snapshot_time:
        now_utc = datetime.fromisoformat(snapshot_time)
    else:
        now_utc = datetime.utcnow().replace(microsecond=0)

    perform_rollup_for_instrument(instrument_id, expiry, now_utc)

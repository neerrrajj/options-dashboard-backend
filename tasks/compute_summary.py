import logging

from db import SessionLocal
from config import INSTRUMENTS
from celery_config import celery_app
from models import OCMinuteSnapshot, OCSummary

logger = logging.getLogger(__name__)

@celery_app.task
def oc_summary_task(instrument_id, expiry, ist_minute):
    db = SessionLocal()

    try:
        rows = db.query(OCMinuteSnapshot).filter_by(
            instrument=instrument_id,
            expiry=expiry,
            ist_minute=ist_minute
        ).all()

        if not rows:
            logger.warning(f"[I-SUMMARY] No data found for {instrument_id} ({expiry}) at IST {ist_minute}")
            return

        underlying = rows[0].underlying_price
        instrument = next(i for i in INSTRUMENTS if i["SECURITY_ID"] == instrument_id)
        strike_range = instrument["STRIKE_RANGE"]
        atm = round(underlying / strike_range) * strike_range

        # Gamma Flip Logic
        rows_sorted = sorted(rows, key=lambda r: r.strike)
        cum_net_gex = 0
        gamma_flip_level = None
        for row in rows_sorted:
            cum_net_gex += row.net_gex or 0
            if cum_net_gex >= 0:
                gamma_flip_level = row.strike
                break

        summary = OCSummary(
            ist_minute=ist_minute,
            instrument=instrument_id,
            expiry=expiry,
            underlying_price=underlying,
            total_net_gex=sum(r.net_gex or 0 for r in rows),
            gamma_flip_level=gamma_flip_level,
            otm_call_vega=sum(r.call_vega or 0 for r in rows if r.strike >= atm),
            otm_put_vega=sum(r.put_vega or 0 for r in rows if r.strike <= atm),
            otm_call_theta=sum(r.call_theta or 0 for r in rows if r.strike >= atm),
            otm_put_theta=sum(r.put_theta or 0 for r in rows if r.strike <= atm),
            otm_call_delta=sum(r.call_delta or 0 for r in rows if r.strike >= atm),
            otm_put_delta=sum(r.put_delta or 0 for r in rows if r.strike <= atm),
        )

        db.add(summary)
        db.commit()
        logger.info(f"[I-SUMMARY] Added summary for {instrument_id} ({expiry}) at IST {ist_minute}")

    except Exception as e:
        db.rollback()
        logger.error(f"[I-SUMMARY] Failed to add summary for {instrument_id} ({expiry}) at IST {ist_minute}: {e}")
    finally:
        db.close()
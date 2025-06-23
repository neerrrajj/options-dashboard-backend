import logging
from sqlalchemy import and_
from datetime import datetime, timedelta

from db import SessionLocal
from models import OCMinuteSnapshot
from celery_config import celery_app
from tasks.compute_summary import oc_summary_task

logger = logging.getLogger(__name__)

@celery_app.task
def save_oc_snapshot_task(instrument, expiry, oc_response, fetch_cycle_count):
    db = SessionLocal()

    try:
        oc, underlying_price = oc_response["oc"], oc_response["last_price"]
        snapshot_time = datetime.utcnow().replace(microsecond=0)
        ist_minute = (snapshot_time + timedelta(hours=5, minutes=30)).replace(second=0, microsecond=0)

        instrument_id = instrument["SECURITY_ID"]
        strike_range = instrument["STRIKE_RANGE"]
        atm = round(underlying_price / strike_range) * strike_range
        lower_bound = atm - 40 * strike_range
        upper_bound = atm + 40 * strike_range

        # Delete existing records for that minute
        deleted_count = db.query(OCMinuteSnapshot).filter(
            and_(
                OCMinuteSnapshot.instrument == instrument_id,
                OCMinuteSnapshot.expiry == expiry,
                OCMinuteSnapshot.ist_minute == ist_minute
            )
        ).delete()
        if deleted_count > 0:
            logger.info(f"[SAVE SNAPSHOT] Deleted {deleted_count} existing rows for {instrument_id} ({expiry}) at IST {ist_minute}")

        for strike_str, chain in oc.items():
            strike = float(strike_str)
            if not (lower_bound <= strike <= upper_bound):
                continue

            ce = chain.get("ce", {})
            pe = chain.get("pe", {})

            row = {
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
            call_gex = (row["call_gamma"] or 0.0) * (row["call_oi"] or 0)
            put_gex = (row["put_gamma"] or 0.0) * (row["put_oi"] or 0)
            net_gex = call_gex - put_gex
            abs_gex = abs(call_gex) + abs(put_gex)

            db_row = OCMinuteSnapshot(
                timestamp=snapshot_time,
                ist_minute=ist_minute,
                **row,
                call_gex=call_gex,
                put_gex=put_gex,
                net_gex=net_gex,
                abs_gex=abs_gex
            )
            db.add(db_row)

        db.commit()
        logger.info(f"[SAVE SNAPSHOT] Saved OCMinuteSnapshot for {instrument_id} ({expiry}) at IST {ist_minute}")

        # Trigger summary task
        oc_summary_task.delay(instrument_id, expiry, ist_minute)

    except Exception as e:
        logger.error(f"[SAVE SNAPSHOT] Error saving OCMinuteSnapshot for {instrument['SECURITY_ID']} ({expiry}): {e}")
        db.rollback()
    finally:
        db.close()

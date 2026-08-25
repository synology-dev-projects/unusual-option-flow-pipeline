import sys
import logging
import traceback
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

# Ensure project root and common-lib are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
COMMON_LIB = PROJECT_ROOT.parent / "common-lib"
if COMMON_LIB.exists() and str(COMMON_LIB) not in sys.path:
    sys.path.insert(0, str(COMMON_LIB))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common_lib.config.main_config import load_config, MainConfig
from common_lib.connectors.nfty import send_ntfy_notification
from src import extract, transform, load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("quant.pipeline.flow.daily_incremental")


def notify_failure(config: Optional[MainConfig], error_message: str) -> None:
    """Dispatches NTFY error notification upon fatal pipeline failure."""
    try:
        endpoint = getattr(config, "ntfy_endpoint", None) if config else None
        if endpoint:
            send_ntfy_notification(
                endpoint=endpoint,
                topic="quant_alerts",
                title="🚨 PIPELINE FAILURE: Options Flow Incremental",
                message=f"Options Flow Incremental pipeline encountered fatal error:\n{error_message}",
                priority=5,
                tags="warning,skull"
            )
            logger.info("Sent failure notification to NTFY.")
    except Exception as ex:
        logger.error(f"Failed to dispatch NTFY alert: {ex}")


def run_daily_incremental() -> int:
    """
    Daily incremental options flow pipeline:
    1. Loads configuration.
    2. Queries latest recorded date from DB (or falls back to 30 days lookback).
    3. Scrapes all displayed symbols and extracts flow beyond cutoff date.
    4. Transforms raw records into normalized DataFrame.
    5. Upserts records into database.
    """
    config = None
    try:
        config = load_config()
        logger.info("Loaded configuration successfully.")
        
        cutoff_date = load.get_latest_recorded_date(config)
        if cutoff_date:
            logger.info(f"Found latest recorded date in DB: {cutoff_date}")
        else:
            cutoff_date = date.today() - timedelta(days=30)
            logger.info(f"No previous data found. Using fallback cutoff_date: {cutoff_date}")
            
        session = extract.get_authenticated_flow_session(config)
        symbols = extract.extract_all_displayed_symbols(config, session=session)
        logger.info(f"Targeting {len(symbols)} universe symbols: {symbols}")
        
        all_raw_records = []
        for sym in symbols:
            logger.info(f"Extracting flow for {sym} (cutoff: {cutoff_date})...")
            recs = extract.extract_flow_for_symbol(config, sym, cutoff_date=cutoff_date, session=session)
            if recs:
                logger.info(f"  -> Extracted {len(recs)} flow records for {sym}.")
                all_raw_records.extend(recs)
                
        if not all_raw_records:
            logger.info("Zero new flow records detected beyond cutoff date. Pipeline finished cleanly.")
            return 0
            
        df_clean = transform.transform_flow_records(all_raw_records)
        logger.info(f"Successfully transformed {len(df_clean)} flow records.")
        
        rows_inserted = load.run(config, df_clean, write_mode="upsert")
        logger.info(f"Daily incremental pipeline completed successfully. Rows upserted: {rows_inserted}")
        return 0
        
    except Exception as ex:
        err_msg = f"{ex}\n{traceback.format_exc()}"
        logger.critical(f"Fatal error in daily incremental pipeline: {err_msg}")
        notify_failure(config, str(ex))
        return 1


def main() -> None:
    exit_code = run_daily_incremental()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

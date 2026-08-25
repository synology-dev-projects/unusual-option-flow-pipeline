import sys
import logging
import argparse
import traceback
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional

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
logger = logging.getLogger("quant.pipeline.flow.manual_historical")


def notify_failure(config: Optional[MainConfig], error_message: str) -> None:
    """Dispatches NTFY error notification upon fatal manual historical backfill failure."""
    try:
        endpoint = getattr(config, "ntfy_endpoint", None) if config else None
        if endpoint:
            send_ntfy_notification(
                endpoint=endpoint,
                topic="quant_alerts",
                title="🚨 PIPELINE FAILURE: Options Flow Historical",
                message=f"Options Flow Historical backfill encountered fatal error:\n{error_message}",
                priority=5,
                tags="warning,skull"
            )
            logger.info("Sent failure notification to NTFY.")
    except Exception as ex:
        logger.error(f"Failed to dispatch NTFY alert: {ex}")


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Historical Backfill for Unusual Options Flow")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Lookback days to fetch (default: None for full 2-year history up to 730 days)"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols to backfill (default: all displayed symbols)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="upsert",
        choices=["upsert", "overwrite", "ignore"],
        help="Database write mode ('upsert', 'overwrite', or 'ignore', default: 'upsert')"
    )
    return parser.parse_args(args)


def run_manual_historical(
    days: Optional[int] = None,
    symbols: Optional[str] = None,
    mode: str = "upsert"
) -> int:
    """
    Executes a manual historical backfill for specified or all displayed symbols.
    """
    config = None
    try:
        config = load_config()
        logger.info(f"Starting manual historical backfill (days={days}, symbols={symbols}, mode={mode})")
        
        session = extract.get_authenticated_flow_session(config)
        
        if symbols:
            target_symbols = [
                s.strip().upper().replace("$", "")
                for s in symbols.split(",")
                if s.strip()
            ]
        else:
            target_symbols = extract.extract_all_displayed_symbols(config, session=session)
            
        logger.info(f"Targeting {len(target_symbols)} symbols: {target_symbols}")
        
        if days is not None:
            cutoff_date = date.today() - timedelta(days=days)
        else:
            # Full history up to 2 years
            cutoff_date = date.today() - timedelta(days=730)
            
        logger.info(f"Historical cutoff date: {cutoff_date}")
        
        all_raw_records = []
        for sym in target_symbols:
            logger.info(f"Fetching historical flow for {sym}...")
            recs = extract.extract_flow_for_symbol(config, sym, cutoff_date=cutoff_date, session=session)
            if recs:
                logger.info(f"  -> Retrieved {len(recs)} flow records for {sym}.")
                all_raw_records.extend(recs)
                
        if not all_raw_records:
            logger.info("No flow records found for the requested criteria. Pipeline finished cleanly.")
            return 0
            
        df_clean = transform.transform_flow_records(all_raw_records)
        logger.info(f"Transformed {len(df_clean)} records across {len(target_symbols)} symbols.")
        
        rows_inserted = load.run(config, df_clean, write_mode=mode)
        logger.info(f"Historical backfill completed successfully. Rows persisted: {rows_inserted}")
        return 0
        
    except Exception as ex:
        err_msg = f"{ex}\n{traceback.format_exc()}"
        logger.critical(f"Fatal error in manual historical backfill: {err_msg}")
        notify_failure(config, str(ex))
        return 1


def main() -> None:
    args = parse_args()
    exit_code = run_manual_historical(days=args.days, symbols=args.symbols, mode=args.mode)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

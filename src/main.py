import logging
import sys
from datetime import datetime, date, timedelta
from typing import List, Optional
from common_lib.config.main_config import load_config
from common_lib.flow import extract, transform, load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("quant.pipeline.flow.main")

BENCHMARK_UNIVERSE = [
    "SPY", "QQQ", "IWM", "NVDA", "AAPL", "TSLA", "META", "AMZN", "GOOGL", "MSFT",
    "PLTR", "AMD", "TSM", "SMCI", "COIN", "CRWD", "NFLX", "AVGO", "ARM", "MSTR"
]

def run_pipeline(symbols: Optional[List[str]] = None, force_backfill_days: Optional[int] = None) -> int:
    """
    Main pipeline entry point:
    1. Queries MAX(TRADE_DATE) from Oracle DB (UNUSUAL_OPTION_FLOW_TE).
    2. Scrapes new flow from TradingEdge for universe where trade_date > cutoff_date.
    3. Transforms and validates records.
    4. Upserts to Oracle DB with idempotent MERGE INTO.
    """
    config = load_config()
    target_symbols = symbols or BENCHMARK_UNIVERSE
    
    # 1. Determine incremental cutoff date
    latest_db_date = load.get_latest_recorded_date(config)
    
    if force_backfill_days:
        cutoff_date = date.today() - timedelta(days=force_backfill_days)
        logger.info(f"Force backfill mode: using cutoff_date = {cutoff_date}")
    elif latest_db_date:
        cutoff_date = latest_db_date
        logger.info(f"Incremental mode: found MAX(TRADE_DATE) in DB = {cutoff_date}")
    else:
        cutoff_date = date.today() - timedelta(days=30)
        logger.info(f"Initial backfill mode (empty DB): using cutoff_date = {cutoff_date}")
        
    # 2. Extract
    session = extract.get_authenticated_flow_session(config)
    all_raw_records = []
    
    for sym in target_symbols:
        logger.info(f"Extracting flow for {sym} (cutoff: {cutoff_date})...")
        recs = extract.extract_flow_for_symbol(config, sym, cutoff_date=cutoff_date, session=session)
        if recs:
            logger.info(f"  -> Found {len(recs)} new flow records for {sym}.")
            all_raw_records.extend(recs)
            
    if not all_raw_records:
        logger.info("Zero new flow records detected beyond cutoff date. Pipeline finished.")
        return 0
        
    # 3. Transform
    df_clean = transform.transform_flow_records(all_raw_records)
    logger.info(f"Transformed {len(df_clean)} records. Columns: {list(df_clean.columns)}")
    
    # 4. Load
    rows_inserted = load.run(config, df_clean, write_mode="upsert")
    logger.info(f"Pipeline completed successfully. Total records upserted: {rows_inserted}")
    return rows_inserted

if __name__ == "__main__":
    run_pipeline()

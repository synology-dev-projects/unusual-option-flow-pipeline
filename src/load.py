import logging
from datetime import datetime, date
from typing import Optional
import pandas as pd
import sqlalchemy as sa
from common_lib.config.main_config import MainConfig
import common_lib.connectors.oracle as oracle
import common_lib.connectors.postgres as postgres

logger = logging.getLogger("quant.pipeline.flow.load")

def get_latest_recorded_date(config: MainConfig) -> Optional[date]:
    """
    Queries DB (PostgreSQL or Oracle) to find the highest TRADE_DATE in unusual_option_flow_te.
    Returns None if the table is empty or does not exist.
    """
    db_type = getattr(config, "db_type", "postgres").lower()
    table_name = getattr(config, "oracle_unusual_flow_table_name", "UNUSUAL_OPTION_FLOW_TE")
    if db_type == "postgres":
        table_name = table_name.lower()
        
    query = f"SELECT MAX(TRADE_DATE) AS MAX_DATE FROM {table_name}"
    try:
        if db_type == "postgres":
            df = postgres.sql(config, query)
        else:
            df = oracle.sql(config, query)
            
        if not df.empty and "MAX_DATE" in df.columns:
            val = df["MAX_DATE"].iloc[0]
            if pd.notna(val):
                if isinstance(val, (datetime, pd.Timestamp)):
                    return val.date()
                elif isinstance(val, date):
                    return val
                return pd.to_datetime(val).date()
    except Exception as ex:
        logger.warning(f"Could not retrieve MAX(TRADE_DATE) from {table_name}: {ex}")
    return None

def run(config: MainConfig, df: pd.DataFrame, write_mode: str = "upsert") -> int:
    """
    Persists flow records to PostgreSQL (default) or Oracle DB.
    """
    db_type = getattr(config, "db_type", "postgres").lower()
    table_name = getattr(config, "oracle_unusual_flow_table_name", "UNUSUAL_OPTION_FLOW_TE")
    primary_keys = getattr(config, "oracle_unusual_flow_pks", ["FLOW_ID"])
    
    if df.empty:
        logger.info("DataFrame is empty. Skipping DB insert.")
        return 0
        
    logger.info(f"Pushing {len(df)} flow records to '{table_name}' on {db_type.upper()} with mode='{write_mode}'...")
    if db_type == "postgres":
        postgres.write_to_postgres_upsert(
            config=config,
            df=df,
            table_name=table_name.lower(),
            pks=[k.lower() for k in primary_keys]
        )
    else:
        oracle.insert_into_table(
            config=config,
            df=df,
            table_name=table_name,
            write_mode=write_mode,
            primary_keys=primary_keys
        )
    logger.info(f"Successfully upserted {len(df)} records into {table_name}.")
    return len(df)


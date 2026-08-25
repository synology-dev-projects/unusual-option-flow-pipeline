import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timedelta
import pandas as pd

from src.extract import extract_all_displayed_symbols, BENCHMARK_UNIVERSE
from src.scripts.daily_incremental import run_daily_incremental, notify_failure as notify_incremental_failure
from src.scripts.manual_historical import run_manual_historical, parse_args, notify_failure as notify_historical_failure
from src.scripts.verify_vertical import run_vertical_slice_test


MOCK_HTML_PAGE = """
<html>
<body>
    <a href="Symbol.aspx?Id=MSTR">MSTR</a>
    <a href="/Symbol.aspx?Id=NVDA">NVDA</a>
    <a href="Symbol.aspx?Id=CUSTOM_CO">CUSTOM</a>
</body>
</html>
"""


def test_extract_all_displayed_symbols():
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = MOCK_HTML_PAGE
    mock_session.get.return_value = mock_resp

    mock_config = MagicMock()
    symbols = extract_all_displayed_symbols(mock_config, session=mock_session)

    assert "NVDA" in symbols
    assert "CUSTOM_CO" in symbols
    assert "SPY" in symbols  # From BENCHMARK_UNIVERSE
    for b in BENCHMARK_UNIVERSE:
        assert b in symbols
    assert symbols == sorted(list(set(symbols)))


@patch("src.load.run")
@patch("src.transform.transform_flow_records")
@patch("src.extract.extract_flow_for_symbol")
@patch("src.extract.extract_all_displayed_symbols")
@patch("src.extract.get_authenticated_flow_session")
@patch("src.load.get_latest_recorded_date")
@patch("src.scripts.daily_incremental.load_config")
def test_daily_incremental_success(
    mock_load_config,
    mock_get_latest_date,
    mock_get_session,
    mock_extract_symbols,
    mock_extract_flow,
    mock_transform,
    mock_load_run
):
    mock_config = MagicMock()
    mock_load_config.return_value = mock_config
    mock_get_latest_date.return_value = date(2026, 8, 20)
    mock_extract_symbols.return_value = ["NVDA", "SPY"]
    mock_extract_flow.return_value = [{"symbol": "NVDA", "strike": "130", "premium": "1M"}]
    mock_df = pd.DataFrame([{"flow_id": "123", "symbol": "NVDA"}])
    mock_transform.return_value = mock_df
    mock_load_run.return_value = 1

    exit_code = run_daily_incremental()
    assert exit_code == 0
    mock_extract_symbols.assert_called_once()
    assert mock_extract_flow.call_count == 2
    mock_transform.assert_called_once()
    mock_load_run.assert_called_once_with(mock_config, mock_df, write_mode="upsert")


@patch("src.transform.transform_flow_records")
@patch("src.extract.extract_flow_for_symbol")
@patch("src.extract.extract_all_displayed_symbols")
@patch("src.extract.get_authenticated_flow_session")
@patch("src.load.get_latest_recorded_date")
@patch("src.scripts.daily_incremental.load_config")
def test_daily_incremental_zero_records(
    mock_load_config,
    mock_get_latest_date,
    mock_get_session,
    mock_extract_symbols,
    mock_extract_flow,
    mock_transform
):
    mock_config = MagicMock()
    mock_load_config.return_value = mock_config
    mock_get_latest_date.return_value = None
    mock_extract_symbols.return_value = ["AAPL"]
    mock_extract_flow.return_value = []

    exit_code = run_daily_incremental()
    assert exit_code == 0
    mock_transform.assert_not_called()


@patch("src.scripts.daily_incremental.send_ntfy_notification")
@patch("src.extract.extract_all_displayed_symbols", side_effect=RuntimeError("Web scrape timeout"))
@patch("src.extract.get_authenticated_flow_session")
@patch("src.load.get_latest_recorded_date")
@patch("src.scripts.daily_incremental.load_config")
def test_daily_incremental_failure_dispatches_ntfy(
    mock_load_config,
    mock_get_latest_date,
    mock_get_session,
    mock_extract_symbols,
    mock_send_ntfy
):
    mock_config = MagicMock()
    mock_config.ntfy_endpoint = "https://ntfy.example.com"
    mock_load_config.return_value = mock_config

    exit_code = run_daily_incremental()
    assert exit_code == 1
    mock_send_ntfy.assert_called_once()
    call_kwargs = mock_send_ntfy.call_args[1]
    assert call_kwargs["topic"] == "quant_alerts"
    assert "🚨 PIPELINE FAILURE: Options Flow Incremental" in call_kwargs["title"]


def test_manual_historical_parse_args():
    args = parse_args(["--days", "45", "--symbols", "NVDA,AAPL", "--mode", "overwrite"])
    assert args.days == 45
    assert args.symbols == "NVDA,AAPL"
    assert args.mode == "overwrite"

    args_default = parse_args([])
    assert args_default.days is None
    assert args_default.symbols is None
    assert args_default.mode == "upsert"


@patch("src.load.run")
@patch("src.transform.transform_flow_records")
@patch("src.extract.extract_flow_for_symbol")
@patch("src.extract.get_authenticated_flow_session")
@patch("src.scripts.manual_historical.load_config")
def test_manual_historical_success_with_symbols(
    mock_load_config,
    mock_get_session,
    mock_extract_flow,
    mock_transform,
    mock_load_run
):
    mock_config = MagicMock()
    mock_load_config.return_value = mock_config
    mock_extract_flow.return_value = [{"symbol": "PLTR", "strike": "180", "premium": "5M"}]
    mock_df = pd.DataFrame([{"flow_id": "f1", "symbol": "PLTR"}])
    mock_transform.return_value = mock_df
    mock_load_run.return_value = 1

    exit_code = run_manual_historical(days=60, symbols="PLTR, TSLA", mode="overwrite")
    assert exit_code == 0
    assert mock_extract_flow.call_count == 2
    mock_load_run.assert_called_once_with(mock_config, mock_df, write_mode="overwrite")


@patch("src.scripts.manual_historical.send_ntfy_notification")
@patch("src.extract.extract_flow_for_symbol", side_effect=RuntimeError("Scrape failed"))
@patch("src.extract.get_authenticated_flow_session")
@patch("src.scripts.manual_historical.load_config")
def test_manual_historical_failure_dispatches_ntfy(
    mock_load_config,
    mock_get_session,
    mock_extract_flow,
    mock_send_ntfy
):
    mock_config = MagicMock()
    mock_config.ntfy_endpoint = "https://ntfy.example.com"
    mock_load_config.return_value = mock_config

    exit_code = run_manual_historical(symbols="NVDA")
    assert exit_code == 1
    mock_send_ntfy.assert_called_once()
    call_kwargs = mock_send_ntfy.call_args[1]
    assert call_kwargs["topic"] == "quant_alerts"
    assert "🚨 PIPELINE FAILURE: Options Flow Historical" in call_kwargs["title"]


@patch("src.scripts.verify_vertical.postgres.get_unusual_flow")
@patch("src.load.run")
@patch("src.extract.extract_flow_for_symbol")
@patch("src.extract.get_authenticated_flow_session")
@patch("src.scripts.verify_vertical.load_config")
def test_verify_vertical_slice_mocked(
    mock_load_config,
    mock_get_session,
    mock_extract_flow,
    mock_load_run,
    mock_get_unusual_flow
):
    mock_config = MagicMock()
    mock_load_config.return_value = mock_config
    mock_extract_flow.return_value = [
        {
            "trade_date": "8/24/26",
            "order_type": "Buy Call",
            "symbol": "NVDA",
            "strike": "130.00 (2 %)",
            "exp": "9/18/26",
            "oi": "1000",
            "premium": "5M",
            "net_score": 1.0
        }
    ]
    mock_load_run.side_effect = lambda cfg, df, write_mode="upsert": 0 if (df is None or df.empty) else len(df)
    mock_get_unusual_flow.return_value = pd.DataFrame([

        {
            "FLOW_ID": "0123456789abcdef0123456789abcdef",
            "TRADE_DATE": "2026-08-24",
            "SYMBOL": "NVDA",
            "ORDER_TYPE": "BUY_CALL",
            "STRIKE_PRICE": 130.0,
            "STRIKE_OTM_PCT": 2.0,
            "EXPIRATION_DATE": "2026-09-18",
            "OPEN_INTEREST": 1000,
            "IS_UNUSUAL_OI": 0,
            "PREMIUM": 5_000_000.0,
            "NET_SCORE": 1.0,
            "CREATED_AT": datetime.now()
        }
    ])

    exit_code = run_vertical_slice_test()
    assert exit_code == 0

import pytest
from unittest.mock import patch, MagicMock
from datetime import date
from src.extract import (
    get_authenticated_flow_session,
    parse_html_flow_table,
    extract_flow_for_symbol,
    extract_all_displayed_symbols,
    BENCHMARK_UNIVERSE
)

SAMPLE_HTML = """
<html>
<body>
    <div class="score">Net Score: +1.5</div>
    <table>
        <tr><th>Trade Date</th><th>Order Type</th><th>Symbol</th><th>Strike</th><th>Exp</th><th>OI</th><th>Premium</th></tr>
        <tr><td>8/24/26</td><td>CBuy Call</td><td>NVDA</td><td>130.00 (2 %)</td><td>9/18/26</td><td>1000</td><td>5M</td></tr>
        <tr><td>8/20/26</td><td>PBuy Put</td><td>NVDA</td><td>120.00 (-5 %)</td><td>9/18/26</td><td>2000 ⚠️</td><td>2.5M</td></tr>
    </table>
</body>
</html>
"""

def test_parse_html_flow_table_details():
    records, net_score = parse_html_flow_table(SAMPLE_HTML, symbol="NVDA")
    assert len(records) == 2
    assert net_score == 1.5
    assert records[0]["order_type"] == "Buy Call"
    assert records[0]["symbol"] == "NVDA"
    assert records[1]["order_type"] == "Buy Put"
    assert records[1]["is_unusual_oi"] == 1


@patch("requests.Session.get")
@patch("requests.Session.post")
def test_get_authenticated_flow_session(mock_post, mock_get):
    mock_config = MagicMock()
    mock_config.te_user_agent = "Mozilla/5.0"
    mock_config.te_option_login_gate = "https://flow.tradingedge.club/Login.aspx"
    mock_config.te_pass = MagicMock(get_secret_value=lambda: "secret_pass")

    mock_resp = MagicMock()
    mock_resp.text = '<html><input id="__VIEWSTATE" value="v1"/><input id="__EVENTVALIDATION" value="e1"/></html>'
    mock_resp.url = "https://flow.tradingedge.club/Login.aspx"
    mock_get.return_value = mock_resp

    sess = get_authenticated_flow_session(mock_config)
    assert sess is not None
    mock_post.assert_called_once()


@patch("src.extract.get_authenticated_flow_session")
def test_extract_flow_for_symbol_with_cutoff(mock_get_session):
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = SAMPLE_HTML
    mock_session.get.return_value = mock_resp
    mock_get_session.return_value = mock_session

    mock_config = MagicMock()
    # Cutoff 8/22/2026 -> only 8/24/2026 record should remain
    records = extract_flow_for_symbol(mock_config, "NVDA", cutoff_date=date(2026, 8, 22), session=mock_session)
    assert len(records) == 1
    assert records[0]["trade_date"] == "8/24/26"


@patch("src.extract.get_authenticated_flow_session")
def test_extract_all_displayed_symbols(mock_get_session):
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = '<html><a href="Symbol.aspx?Id=TSLA">TSLA</a><a href="Symbol.aspx?Id=COIN">COIN</a></html>'
    mock_session.get.return_value = mock_resp
    mock_get_session.return_value = mock_session

    mock_config = MagicMock()
    symbols = extract_all_displayed_symbols(mock_config, session=mock_session)
    assert "TSLA" in symbols
    assert "COIN" in symbols
    for b in BENCHMARK_UNIVERSE:
        assert b in symbols

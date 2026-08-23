import pytest
from datetime import date, datetime
import pandas as pd
from src.transform import parse_premium_str, parse_strike_and_otm, generate_flow_id, transform_flow_records
from src.extract import parse_html_flow_table

SAMPLE_FLOW_HTML = """
<html>
<body>
    <div class="net-score-card">Net Score: <span>+1</span></div>
    <table>
        <tr>
            <th>Trade Date</th>
            <th>Order Type</th>
            <th>Symbol</th>
            <th>Strike</th>
            <th>Exp</th>
            <th>OI</th>
            <th>Premium</th>
        </tr>
        <tr>
            <td>8/17/26</td>
            <td>Buy Call</td>
            <td>PLTR</td>
            <td>185.00 (7 %)</td>
            <td>12/17/27</td>
            <td>4114</td>
            <td>14.2M</td>
        </tr>
        <tr>
            <td>8/13/26</td>
            <td>Buy Call</td>
            <td>PLTR</td>
            <td>180.00 (1 %)</td>
            <td>12/17/27</td>
            <td>1957 ⚠️</td>
            <td>21.7M</td>
        </tr>
        <tr>
            <td>8/11/26</td>
            <td>Buy Put</td>
            <td>PLTR</td>
            <td>140.00 (-20 %)</td>
            <td>12/18/26</td>
            <td>32893</td>
            <td>18.3M</td>
        </tr>
    </table>
</body>
</html>
"""

def test_parse_premium_str():
    assert parse_premium_str("14.2M") == 14_200_000.0
    assert parse_premium_str("780K") == 780_000.0
    assert parse_premium_str("$1.85M") == 1_850_000.0
    assert parse_premium_str("500") == 500.0
    assert parse_premium_str(None) == 0.0

def test_parse_strike_and_otm():
    stk, otm = parse_strike_and_otm("185.00 (7 %)")
    assert stk == 185.0
    assert otm == 7.0
    
    stk_neg, otm_neg = parse_strike_and_otm("140.00 (-20 %)")
    assert stk_neg == 140.0
    assert otm_neg == -20.0
    
    stk_plain, otm_plain = parse_strike_and_otm("250.50")
    assert stk_plain == 250.5
    assert otm_plain is None

def test_parse_html_flow_table():
    records, net_score = parse_html_flow_table(SAMPLE_FLOW_HTML, symbol="PLTR")
    assert len(records) == 3
    assert net_score == 1.0
    assert records[0]["symbol"] == "PLTR"
    assert records[0]["order_type"] == "Buy Call"
    assert records[1]["is_unusual_oi"] == 1
    assert records[2]["order_type"] == "Buy Put"

def test_transform_flow_records():
    records, net_score = parse_html_flow_table(SAMPLE_FLOW_HTML, symbol="PLTR")
    df = transform_flow_records(records, net_score=net_score)
    
    assert len(df) == 3
    assert "flow_id" in df.columns
    assert df["symbol"].iloc[0] == "PLTR"
    assert df["order_type"].iloc[0] == "BUY_CALL"
    assert df["strike_price"].iloc[0] == 185.0
    assert df["strike_otm_pct"].iloc[0] == 7.0
    assert df["premium"].iloc[0] == 14_200_000.0
    assert df["is_unusual_oi"].iloc[1] == 1
    assert df["order_type"].iloc[2] == "BUY_PUT"

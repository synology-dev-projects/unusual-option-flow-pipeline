import logging
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup
from common_lib.config.main_config import MainConfig

logger = logging.getLogger("quant.pipeline.flow.extract")

def get_authenticated_flow_session(config: MainConfig) -> requests.Session:
    """
    Creates an authenticated requests.Session for TradingEdge Flow.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": getattr(config, "te_user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
        "Referer": "https://flow.tradingedge.club/"
    })
    
    login_url = getattr(config, "te_option_login_gate", "https://flow.tradingedge.club/Login.aspx?ReturnUrl=%2fdefault.aspx")
    te_pass = config.te_pass.get_secret_value() if hasattr(config.te_pass, "get_secret_value") else str(config.te_pass)
    
    try:
        r_gate = session.get(login_url, timeout=15)
        soup = BeautifulSoup(r_gate.text, "html.parser")
        
        payload = {}
        for input_tag in soup.find_all("input"):
            name = input_tag.get("name")
            if name:
                payload[name] = input_tag.get("value", "")
                
        for k in payload:
            if "pass" in k.lower() or "txt" in k.lower():
                payload[k] = te_pass
                
        session.post(login_url, data=payload, timeout=15)
    except Exception as ex:
        logger.warning(f"Flow login gate encounter: {ex}")
        
    return session

def parse_html_flow_table(html_content: str, symbol: Optional[str] = None) -> tuple[List[Dict[str, Any]], Optional[float]]:
    """
    Parses TradingEdge Flow HTML table into raw record dictionaries.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    records = []
    net_score = None
    
    score_elem = soup.find(class_=re.compile(r"net.*score|score", re.I)) or soup.find(string=re.compile(r"Net Score", re.I))
    if score_elem:
        parent = score_elem.parent
        match = re.search(r"([\+\-]?\d+(?:\.\d+)?)", parent.get_text() if parent else "")
        if match:
            try:
                net_score = float(match.group(1))
            except Exception:
                pass
                
    table = soup.find("table")
    if not table:
        return records, net_score
        
    rows = table.find_all("tr")
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
            
        texts = [td.get_text(strip=True) for td in tds]
        
        raw_trade_date = texts[0]
        raw_order_type = texts[1]
        raw_symbol = texts[2] if len(texts) > 2 else (symbol or "")
        raw_strike = texts[3] if len(texts) > 3 else ""
        raw_exp = texts[4] if len(texts) > 4 else ""
        raw_oi = texts[5] if len(texts) > 5 else ""
        raw_premium = texts[6] if len(texts) > 6 else ""
        
        is_unusual_oi = 1 if (tds[5].find("svg") or "▲" in raw_oi or "⚠️" in raw_oi) else 0
        
        records.append({
            "trade_date": raw_trade_date,
            "order_type": raw_order_type,
            "symbol": raw_symbol or symbol,
            "strike": raw_strike,
            "exp": raw_exp,
            "oi": raw_oi,
            "is_unusual_oi": is_unusual_oi,
            "premium": raw_premium,
            "net_score": net_score
        })
        
    return records, net_score

def extract_flow_for_symbol(config: MainConfig, symbol: str, cutoff_date: Optional[date] = None, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    Fetches raw flow records for a single symbol from TradingEdge Flow.
    """
    sess = session or get_authenticated_flow_session(config)
    url = f"https://flow.tradingedge.club/Symbol/{symbol.strip().upper()}"
    
    try:
        resp = sess.get(url, timeout=20)
        if resp.status_code == 200:
            records, score = parse_html_flow_table(resp.text, symbol=symbol.strip().upper())
            
            if cutoff_date:
                filtered = []
                for rec in records:
                    try:
                        rec_date = pd.to_datetime(rec["trade_date"]).date()
                        if rec_date > cutoff_date:
                            filtered.append(rec)
                    except Exception:
                        filtered.append(rec)
                return filtered
            return records
    except Exception as ex:
        logger.error(f"Error fetching flow for {symbol}: {ex}")
    return []

# `unusual-option-flow-pipeline`

An automated ETL pipeline that monitors, scrapes, cleans, and analyzes unusual options volume, institutional dark pool orders, block trades, and aggressive sweeps from TradingEdge.

---

## 🏛️ Architecture & Modules

```
unusual-option-flow-pipeline/
├── src/
│   ├── extract.py         # Interfaces with TradingEdge option flow streams via common-lib
│   ├── transform.py       # Cleans volume, open interest (OI) ratio, premium, and sentiment
│   └── load.py            # Persists institutional flow records into Oracle Database
├── tests/                 # Unit & regression tests
├── Dockerfile             # Container build specification
└── README.md
```

---

## 🎯 Design Goals

1. **Institutional Sweep Detection**:
   - Captures high-conviction call/put sweeps with premium size $> \$100\text{k}$ and volume significantly exceeding open interest.
2. **Standardized Ingestion**:
   - Persists cleaned options flow records into database tables for historical pattern recognition and AI agent queries.
3. **Common-Lib Integration**:
   - Leverages `common_lib.connectors.tradingedge.optionflow` for authenticated session pooling and scraping logic.
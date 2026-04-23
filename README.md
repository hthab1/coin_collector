# Multi-symbol Bitget trade collector

This stripped codebase only collects Bitget public trade data and writes it to CSV.

## Collected symbols
- PIPPINUSDT
- XRPUSDT

Add more later by editing `INST_IDS` in `settings.py`.

## Output
CSV files are written to the `csv/` folder in this format:
- `csv/PIPPINUSDT_YYYY-MM-DD.csv`
- `csv/XRPUSDT_YYYY-MM-DD.csv`

Each CSV row contains:
- `ts`
- `price`
- `size`
- `side`
- `trade_id`

## Run
```bash
pip install -r requirements.txt
python main.py
```

## Notes
- One websocket receiver is started per symbol.
- If the CSV queue fills up, the oldest queued rows are dropped to keep the collector alive.
- This version removes trading, state, segmentation, API server, leverage config, and all execution logic.

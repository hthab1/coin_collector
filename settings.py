from pathlib import Path

WS_URL = "wss://ws.bitget.com/v2/ws/public"
INST_IDS = [
    "XRPUSDT",
    "SOLUSDT",
    "DOGEUSDT",
    "PEPEUSDT",
    "HYPEUSDT",
    "ZECUSDT",
    "RAVEUSDT",
    "SPKUSDT",
    "SUIUSDT",
]

CSV_DIR = Path("csv")

BATCH_SIZE = 500
FLUSH_EVERY_SEC = 1.0
QUEUE_MAX = 50_000
RECONNECT_DELAY_SEC = 3
HEARTBEAT_INTERVAL_SEC = 25

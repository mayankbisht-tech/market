import os
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL=os.getenv("DATABASE_URL")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET     = os.getenv("R2_BUCKET", "crypto-archive")

SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt",  "xrpusdt",
    "dogeusdt","adausdt", "avaxusdt","maticusdt", "ltcusdt"
]
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)
BATCH_SIZE = 1000
RECONNECT_HOURS = 23

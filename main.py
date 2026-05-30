import json
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import websockets
from fastapi import FastAPI
from config  import BINANCE_WS_URL, BATCH_SIZE, RECONNECT_HOURS, SYMBOLS
from candles import CandleBuilder
from archive import archive_yesterday
import database


candle_builder = CandleBuilder()

pending_batch: list[dict] = []


def parse_trade(raw: str):
    msg = json.loads(raw)
    d = msg.get("data", {})

    if d.get("e") != "trade":
        return None

    return {
        "symbol":        d["s"].lower(),
        "price":         float(d["p"]),
        "qty":           float(d["q"]),
        "ts":            datetime.fromtimestamp(d["T"] / 1000, tz=timezone.utc),
        "trade_id":      d["t"],
        "event_time":    datetime.fromtimestamp(d["E"] / 1000, tz=timezone.utc),
        "is_buyer_maker": d["m"],
    }


async def flush_batch():
    global pending_batch
    if not pending_batch:
        return
    to_flush   = pending_batch.copy()
    pending_batch = []
    await database.insert_candles_batch(to_flush)
    print(f"flushed {len(to_flush)} candles to db")



async def binance_stream():
    global pending_batch

    while True:
        reconnect_at = asyncio.get_event_loop().time() + (RECONNECT_HOURS * 3600)

        try:
            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                print("connected to binance")

                while True:

                    if asyncio.get_event_loop().time() >= reconnect_at:
                        print("23h reached — reconnecting to binance cleanly")
                        await flush_batch()
                        break

                    raw = await ws.recv()

                    trade = parse_trade(raw)
                    if not trade:
                        continue

                    completed = candle_builder.process_trade(
                        symbol=trade["symbol"],
                        price=trade["price"],
                        qty=trade["qty"],
                        ts=trade["ts"],
                    )

                    if completed:
                        pending_batch.append(completed)

                    # flush to db every BATCH_SIZE candles
                    if len(pending_batch) >= BATCH_SIZE:
                        await flush_batch()

        except Exception as e:
            print(f"websocket error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)



async def daily_archive_loop():

    while True:
        now        = datetime.now(timezone.utc)
        tomorrow   = now.replace(hour=0, minute=5, second=0, microsecond=0)
        from datetime import timedelta
        if tomorrow <= now:
            tomorrow = tomorrow + timedelta(days=1)
        wait_secs  = (tomorrow - now).total_seconds()
        print(f"archive job will run in {wait_secs/3600:.1f}h")
        await asyncio.sleep(wait_secs)
        try:
            await archive_yesterday()
        except Exception as e:
            print(f"archive job failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):

    await database.create_tables()

    stream_task  = asyncio.create_task(binance_stream())
    archive_task = asyncio.create_task(daily_archive_loop())
    yield
    stream_task.cancel()
    archive_task.cancel()
    await flush_batch()
    await database.close_pool()

app = FastAPI(lifespan=lifespan)
@app.get("/")
async def root():
    return {"status": "running", "symbols": SYMBOLS}
@app.get("/candles/{symbol}")
async def get_recent_candles(symbol: str, limit: int = 60):
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM candles_1s
            WHERE symbol = $1
            ORDER BY opened_at DESC
            LIMIT $2
            """,
            symbol.lower(), limit,
        )
    return [dict(r) for r in rows]


@app.get("/archive/run")
async def trigger_archive():
    try:
        await archive_yesterday()
        return {"status": "done"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
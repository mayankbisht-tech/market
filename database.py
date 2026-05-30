import asyncpg
from config import DATABASE_URL
_pool = None
async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool
async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
async def create_tables():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS candles_1s (
                symbol      TEXT        NOT NULL,
                opened_at   TIMESTAMPTZ NOT NULL,
                open        NUMERIC(18,8) NOT NULL,
                high        NUMERIC(18,8) NOT NULL,
                low         NUMERIC(18,8) NOT NULL,
                close       NUMERIC(18,8) NOT NULL,
                volume      NUMERIC(24,8) NOT NULL,
                trade_count INT          NOT NULL,
                PRIMARY KEY (symbol, opened_at)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id          BIGSERIAL PRIMARY KEY,
                symbol      TEXT          NOT NULL,
                side        TEXT          NOT NULL,
                price       NUMERIC(18,8) NOT NULL,
                quantity    NUMERIC(18,8) NOT NULL,
                ts          TIMESTAMPTZ   NOT NULL DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS archive_log (
                id           SERIAL PRIMARY KEY,
                archived_at  TIMESTAMPTZ DEFAULT now(),
                date         TEXT NOT NULL,
                rows_saved   INT,
                r2_key       TEXT,
                ok           BOOLEAN DEFAULT false
            )
        """)

    print("tables ready")


async def insert_candles_batch(rows: list[dict]):
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO candles_1s
                (symbol, opened_at, open, high, low, close, volume, trade_count)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol, opened_at) DO UPDATE SET
                high        = GREATEST(candles_1s.high,  EXCLUDED.high),
                low         = LEAST   (candles_1s.low,   EXCLUDED.low),
                close       = EXCLUDED.close,
                volume      = candles_1s.volume + EXCLUDED.volume,
                trade_count = candles_1s.trade_count + EXCLUDED.trade_count
            """,
            [
                (
                    r["symbol"],
                    r["opened_at"],
                    r["open"],
                    r["high"],
                    r["low"],
                    r["close"],
                    r["volume"],
                    r["trade_count"],
                )
                for r in rows
            ],
        )


async def fetch_candles_for_date(date_str: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM candles_1s
            WHERE opened_at::date = $1::date
            ORDER BY symbol, opened_at
            """,
            date_str,
        )
    return [dict(r) for r in rows]


async def delete_candles_for_date(date_str: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM candles_1s WHERE opened_at::date = $1::date",
            date_str,
        )
        await conn.execute("VACUUM candles_1s")
    print(f"deleted candles for {date_str}: {deleted}")


async def log_archive(date_str: str, rows_saved: int, r2_key: str, ok: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO archive_log (date, rows_saved, r2_key, ok)
            VALUES ($1, $2, $3, $4)
            """,
            date_str, rows_saved, r2_key, ok,
        )

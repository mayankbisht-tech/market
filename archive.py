import io
import boto3
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET
import database
def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )
async def archive_yesterday():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    r2_key    = f"candles_1s/{yesterday}.parquet"
    print(f"starting archive for {yesterday}")
    rows = await database.fetch_candles_for_date(yesterday)
    if not rows:
        print(f"no candles found for {yesterday}, skipping")
        return
    df = pd.DataFrame(rows)
    df["opened_at"] = pd.to_datetime(df["opened_at"], utc=True)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression="zstd")
    buffer.seek(0)
    parquet_size = buffer.getbuffer().nbytes
    print(f"parquet size: {parquet_size / 1024:.1f} KB  rows: {len(rows)}")
    ok = False
    try:
        r2 = get_r2_client()
        r2.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=buffer.read(),
            ContentType="application/octet-stream",
        )
        ok = True
        print(f"uploaded to r2: {r2_key}")
    except Exception as e:
        print(f"r2 upload failed: {e}")
    await database.log_archive(
        date_str=yesterday,
        rows_saved=len(rows),
        r2_key=r2_key,
        ok=ok,
    )

    if not ok:
        print("upload failed — NOT deleting from neon. will retry next run.")
        return
    await database.delete_candles_for_date(yesterday)
    print(f"archive complete for {yesterday}")

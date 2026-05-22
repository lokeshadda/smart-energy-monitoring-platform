#!/usr/bin/env python3
import os, json
from datetime import datetime, timedelta, timezone
import redis

# -------- Config --------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Length of each rollup window (minutes). Default = 10
WINDOW_MINUTES = int(os.getenv("ROLLUP_MINUTES", "10"))
# Key prefix to write rollups under (helps keep hourly vs 10-min separate)
ROLLUP_PREFIX = os.getenv("ROLLUP_PREFIX", "tenmin")

# Cities (same as stream job)
CITIES = ["Tampa","Orlando","Miami","StPete","Jacksonville"]

def floor_to_window(dt_utc: datetime, minutes: int) -> datetime:
    """Floor a UTC datetime to the last completed N-minute boundary."""
    # remove seconds/micros
    dt_utc = dt_utc.replace(second=0, microsecond=0)
    # floor minutes
    floored_min = (dt_utc.minute // minutes) * minutes
    return dt_utc.replace(minute=floored_min)

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

    # Use the most recent *completed* N-minute window in UTC
    now = datetime.now(timezone.utc)
    end = floor_to_window(now, WINDOW_MINUTES)          # e.g., 22:50, 23:00, 23:10, ...
    start = end - timedelta(minutes=WINDOW_MINUTES)     # N minutes earlier

    # Redis minute-window keys are city:{city}:window:%Y%m%d%H%M (written by stream_job)
    # Aggregate all minute windows within [start, end)
    bucket_id = start.strftime("%Y%m%dT%H%M")           # e.g., 20251108T2250
    totals = {}

    for city in CITIES:
        total = 0.0
        t = start
        while t < end:
            mkey = f"city:{city}:window:{t.strftime('%Y%m%d%H%M')}"
            if r.exists(mkey):
                data = r.hgetall(mkey)
                if data:
                    # bytes -> str
                    data = {k.decode(): v.decode() for k, v in data.items()}
                    total += float(data.get("total_watts", "0"))
            t += timedelta(minutes=1)

        total = round(total, 2)
        totals[city] = total

        # Write per-city rollup
        city_key = f"{ROLLUP_PREFIX}:{bucket_id}:{city}"
        r.hset(city_key, mapping={
            "city": city,
            "window_minutes": str(WINDOW_MINUTES),
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "total_watts": str(total),
        })
        r.expire(city_key, 7 * 24 * 3600)  # keep a week

    # Combined rollup for quick reads
    all_key = f"{ROLLUP_PREFIX}:{bucket_id}:ALL"
    r.hset(all_key, mapping={
        "window_minutes": str(WINDOW_MINUTES),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "totals_json": json.dumps(totals),
    })
    r.expire(all_key, 7 * 24 * 3600)

    # Log output (useful in Airflow)
    print(json.dumps({
        "bucket": bucket_id,
        "window_minutes": WINDOW_MINUTES,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "totals": totals
    }, indent=2))

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse, os, sys, time
from datetime import datetime, timedelta, timezone
import snowflake.connector

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True, help="e.g. RAW.SOLUTION_BASE")
    p.add_argument("--file", required=True, help="file name or path ending with __Solution.csv")
    p.add_argument("--timeout", type=int, default=1800, help="seconds (default 30m)")
    p.add_argument("--lookback-hours", type=int, default=24)
    args = p.parse_args()

    # Connection from env
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="EPL"
    )

    file_tail = args.file.split("/")[-1]  # match end of path
    start = time.time()
    status = None
    last_row = None

    try:
        while time.time() - start < args.timeout:
            q = f"""
            SELECT file_name, status, row_count, last_load_time
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME=>'{args.table}',
                START_TIME=>DATEADD('HOUR', -{args.lookback_hours}, CURRENT_TIMESTAMP())
            ))
            WHERE file_name ILIKE '%{file_tail}%'
            ORDER BY last_load_time DESC
            LIMIT 1;
            """
            cur = conn.cursor()
            cur.execute("USE SCHEMA EPL.RAW")
            cur.execute(q)
            row = cur.fetchone()
            cur.close()

            if row:
                last_row = row
                status = (row[1] or "").upper()
                if status == "LOADED":
                    print(f"Gate passed: {row[0]} status={status} rows={row[2]} at {row[3]}")
                    return 0
                elif status in ("LOAD_FAILED", "PARTIALLY_LOADED"):
                    print(f"Gate failed: {row[0]} status={status}", file=sys.stderr)
                    return 2

            time.sleep(10)

        print(f"Timeout waiting for {file_tail}. Last seen: {last_row}", file=sys.stderr)
        return 3
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())

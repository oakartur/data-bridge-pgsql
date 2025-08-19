#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, sqlite3, psycopg
from datetime import datetime, timedelta, timezone

def parse_args():
    ap = argparse.ArgumentParser(description="Verificar paridade SQLite vs PostgreSQL")
    ap.add_argument("--sqlite", required=True)
    ap.add_argument("--pgdsn", required=True)
    ap.add_argument("--schema", default="ingest")
    ap.add_argument("--table", default="readings")
    ap.add_argument("--days", type=int, default=7)
    return ap.parse_args()

def main():
    args = parse_args()
    sconn = sqlite3.connect(args.sqlite)
    sconn.row_factory = sqlite3.Row
    pconn = psycopg.connect(args.pgdsn, autocommit=True)

    since = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()

    # Totais por dia
    scur = sconn.execute("""
        SELECT substr(ts,1,10) AS d, COUNT(*) AS n FROM readings
        WHERE ts >= ?
        GROUP BY 1 ORDER BY 1 DESC
    """, (since,))
    sql_counts = dict((r["d"], r["n"]) for r in scur.fetchall())

    with pconn.cursor() as pc:
        pc.execute(f"""
            SELECT to_char(ts, 'YYYY-MM-DD') AS d, COUNT(*) AS n
            FROM {args.schema}.{args.table}
            WHERE ts >= %s
            GROUP BY 1 ORDER BY 1 DESC
        """, (since,))
        pg_counts = dict(pc.fetchall())

    days = sorted(set(sql_counts.keys()).union(pg_counts.keys()), reverse=True)
    print("Dia         | SQLite | Postgres | Δ")
    print("------------+--------+----------+----")
    for d in days:
        a = sql_counts.get(d, 0)
        b = pg_counts.get(d, 0)
        print(f"{d} | {a:6d} | {b:8d} | {b-a:+d}")

if __name__ == "__main__":
    main()

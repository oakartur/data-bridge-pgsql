#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, json, sqlite3, sys, time
from datetime import datetime
import psycopg
from psycopg.rows import tuple_row

def parse_args():
    ap = argparse.ArgumentParser(description="Migrar histórico do SQLite para PostgreSQL/TimescaleDB")
    ap.add_argument("--sqlite", required=True, help="Caminho do arquivo SQLite (ex.: data_ingest.db)")
    ap.add_argument("--pgdsn", required=True, help="DSN do PostgreSQL (ex.: host=... port=5432 dbname=... user=... password=...)")
    ap.add_argument("--schema", default="ingest")
    ap.add_argument("--table", default="readings")
    ap.add_argument("--batch-size", type=int, default=10000)
    ap.add_argument("--since", default=None, help="ISO 8601 (UTC) mínima para migrar (opcional)")
    return ap.parse_args()

def main():
    args = parse_args()

    if not os.path.exists(args.sqlite):
        print(f"SQLite não encontrado: {args.sqlite}", file=sys.stderr)
        sys.exit(2)

    # SQLite
    sconn = sqlite3.connect(args.sqlite)
    sconn.row_factory = sqlite3.Row

    # Postgres
    pconn = psycopg.connect(args.pgdsn, autocommit=True, row_factory=tuple_row)

    schema, table = args.schema, args.table
    min_ts_filter = ""
    params = []

    if args.since:
        min_ts_filter = " AND ts >= ? "
        params.append(args.since)

    # Upsert idempotente por (device_profile, device_name, ts, payload) para reduzir duplicatas.
    # Se quiser uma PK lógica, crie UNIQUE INDEX (device_profile, device_name, ts) antes.
    insert_sql = f"""
        INSERT INTO {schema}.{table}
            (application_name, device_profile, device_name, ts, payload)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT DO NOTHING
    """

    last_id = 0
    total = 0
    failed = 0
    batch = args.batch_size

    # Caminho feliz: percorrer em ordem (id ASC) em lotes
    while True:
        cur = sconn.execute(
            f"""
            SELECT id, application_name, device_profile, device_name, ts, payload
            FROM readings
            WHERE id > ? {min_ts_filter}
            ORDER BY id ASC
            LIMIT ?
            """,
            [last_id] + params + [batch]
        )
        rows = cur.fetchall()
        if not rows:
            break

        batch_last_id = rows[-1]["id"]

        with pconn.cursor() as pc:
            try:
                with pconn.transaction():
                    for r in rows:
                        # Normaliza ts (assume ISO UTC). Timescale aceita ISO com TZ.
                        ts_iso = r["ts"]
                        payload = r["payload"]
                        pc.execute(
                            insert_sql,
                            (
                                r["application_name"],
                                r["device_profile"],
                                r["device_name"],
                                ts_iso,
                                payload,
                            ),
                        )
                total += len(rows)
                last_id = batch_last_id
                print(f"Migrados: {total} (até id={last_id})")
            except Exception as e:
                failed += len(rows)
                last_id = batch_last_id
                print(
                    f"Falha ao migrar lote até id={last_id}: {e}",
                    file=sys.stderr,
                )
                continue

    print(f"Concluído. Total migrado: {total}. Não migrados: {failed}")

if __name__ == "__main__":
    main()

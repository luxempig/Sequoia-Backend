#!/usr/bin/env python3
import os
import psycopg2
import csv
import sys

def create_and_load_table(conn, table_name, csv_path):
    if not os.path.isfile(csv_path):
        print(f"⚠️  CSV not found: {csv_path}", file=sys.stderr)
        return

    with conn.cursor() as cur:
        # 1) Read header row to get column names
        with open(csv_path, newline='') as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                print(f"⚠️  Empty CSV: {csv_path}", file=sys.stderr)
                return

        # 2) Create table if it doesn't exist (all columns as TEXT)
        cols_ddl = ", ".join(f'"{col}" TEXT' for col in headers)
        ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_ddl});'
        cur.execute(ddl)
        conn.commit()

        # 3) Load CSV data via COPY
        cols_list = ", ".join(f'"{col}"' for col in headers)
        copy_sql = (
            f'COPY "{table_name}" ({cols_list}) '
            f'FROM STDIN WITH CSV HEADER'
        )
        with open(csv_path, "r") as f:
            cur.copy_expert(copy_sql, f)
        conn.commit()

        print(f"✅ Imported {csv_path} → {table_name}")

def main():
    # Load DB connection info from environment
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    # Map your env-vars to table names
    tables = {
        "passengers":    os.environ.get("P_PATH"),
        "voyages":       os.environ.get("V_PATH"),
        "voyage_passengers": os.environ.get("PV_PATH"),
    }

    for tbl, path in tables.items():
        create_and_load_table(conn, tbl, path)

    conn.close()

if __name__ == "__main__":
    main()

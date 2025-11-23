#!/usr/bin/env python3
"""
Ingest blockchain events from PostgreSQL → Parquet files
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
import argparse
from pathlib import Path
import sqlalchemy as sq
from sqlalchemy import create_engine
import sqlite3
# ============================= CONFIG =============================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "timedb",
    "user": "postgres",
    # trust auth → no password
}

TABLE_NAME = "ClassifiedEvents"           # Your events table
BLOCK_COLUMN = "block_number"   # Column to chunk on
CHUNK_SIZE = 500001             # Blocks per file
OUTPUT_DIR = Path(f"dataProcessing/record/{TABLE_NAME}")
COMPRESSION = "zstd"            # zstd, snappy, gzip
SCHEMA = {'block_number': 'Int64', 'tx_hash': 'TEXT','type': 'TEXT','amount': 'NUMERIC', 'senders': 'JSON', 'receivers': 'JSON', 'approvals': 'JSON' }
# =================================================================

def get_db_connection():
    return create_engine(
    f"postgresql+psycopg2://"
    f"{DB_CONFIG['user']}@"           # no password
    f"{DB_CONFIG['host']}:"           # host
    f"{DB_CONFIG['port']}/"           # port
    f"{DB_CONFIG['dbname']}"          # dbname
)

def get_block_range(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN({BLOCK_COLUMN}), MAX({BLOCK_COLUMN}) FROM \"{TABLE_NAME}\"")
        min_block, max_block = cur.fetchone()
        return int(min_block or 0), int(max_block or 0)

def get_exported_blocks(output_dir):
    exported = set()
    for file in output_dir.glob("block_*.parquet"):
        try:
            start = int(file.stem.split('_')[1])
            end = int(file.stem.split('_')[2])
            exported.update(range(start, end + 1))
        except:
            pass
    return exported

def export_chunk(conn, start_block, end_block, output_path):
    query = f"""
        SELECT * FROM "{TABLE_NAME}"
        WHERE {BLOCK_COLUMN} BETWEEN %s AND %s
        ORDER BY {BLOCK_COLUMN}
    """
    df = pd.read_sql_table(
    "your_table",
    conn,
    # ← THIS LINE PRESERVES EVERY TYPE
    dtype_backend="numpy_nullable",
    # ← THIS LINE KEEPS 78-digit IDs EXACT
    parse_dates=True,
    coerce_float=False,
).convert_dtypes(
    # force any integer >2⁶³ to string (uint256, token_id, etc.)
    convert_integer=False,
    convert_string=True
)
    dfs = pd.read_sql_table("ClassifiedEvents", conn, chunksize= 100)
    # df = pd.read_sql(query, conn, params=(start_block, end_block), dtype=SCHEMA)
    start = start_block
    
    for df in dfs:
        end = min(start+100, end_block)
        if not df.empty:
            df.to_parquet(OUTPUT_DIR / f"{start}.{end}.parquet", compression=COMPRESSION, index=['block_number'])
        start = end

    if df.empty:
        return 0
    
    df.to_parquet(output_path, compression=COMPRESSION, index=False)
    return len(df)

# def export_chunk(conn, start, end, path):
#     # DuckDB magic: stream + enforce types + write Parquet
#     cols = ",\n    ".join(f'"{col}"' for col in SCHEMA.keys())
#     types = ",\n    ".join(f'"{col}" :: {typ.split()[0]}' for col, typ in SCHEMA.items())
    
#     query = f"""
#         COPY (
#             SELECT {cols}
#             FROM "{TABLE_NAME}"
#             WHERE {BLOCK_COLUMN} BETWEEN {start} AND {end}
#             ORDER BY {BLOCK_COLUMN}
#         ) TO '{path}' 
#         (FORMAT PARQUET, COMPRESSION '{COMPRESSION}', ROW_GROUP_SIZE 100000)
#         WITH (COLUMNS = {{ {', '.join(f"'{c}': '{t.split()[0]}'" for c,t in SCHEMA.items())} }})
#     """
#     conn.execute(query)
#     rows = conn.sql(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
#     return rows

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    print("Connecting to database...")
    conn = get_db_connection()
    
    print("Getting block range...")
    # min_block, max_block = get_block_range(conn)
    min_block = 16088296
    max_block = 37268683
    print(f"Blocks: {min_block:,} → {max_block:,}")
    
    print("Finding already exported blocks...")
    # exported = get_exported_blocks(OUTPUT_DIR)
    total_blocks = max_block - min_block + 1
    # remaining = total_blocks - len(exported & set(range(min_block, max_block + 1)))
    remaining = total_blocks
    print(f"Remaining: {remaining:,} blocks")

    chunks = []
    current = min_block
    while current <= max_block:
        chunk_start = current
        chunk_end = min(current + CHUNK_SIZE - 1, max_block)
        # if all(b in exported for b in range(chunk_start, chunk_end + 1)):
        #     current = chunk_end + 1
        #     continue
        chunks.append((chunk_start, chunk_end))
        current = chunk_end + 1

    print(f"Exporting {len(chunks)} chunks to Parquet...")
    
    total_rows = 0
    for start, end in tqdm(chunks, desc="Chunks", unit="chunk"):
        output_file = OUTPUT_DIR / f"{start}.{end}.parquet"
        rows = export_chunk(conn, start, end, output_file)
        total_rows += rows
        tqdm.write(f"  {output_file.name} → {rows:,} rows")

    conn.close()
    print(f"\nDone! Exported {total_rows:,} rows to {OUTPUT_DIR}")



# ========= CONFIG =========
PG_DSN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "timedb",
    "user": "postgres",
    # trust auth → no password
}
DUCKDB_FILE = "mydb.duckdb"
SCHEMAS = ["public"]          # add more: ["public", "sales"]
EXCLUDE_TABLES = []           # e.g. ["temp_log"]
MAX_WORKERS = 4               # parallel copy
# ==========================

if __name__ == "__main__":
    # global CHUNK_SIZE, OUTPUT_DIR
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Blocks per file")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR), help="Output directory")
    args = parser.parse_args()
    

    CHUNK_SIZE = args.chunk_size
    OUTPUT_DIR = Path(args.output)
    
    main()
import time, json, uuid
from pathlib import Path
from datetime import datetime, UTC
import duckdb

# -----------------------------
# INPUT: dossier 10GB (CSV)
# -----------------------------
DATA_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\raw\10GB")
PATTERN = (DATA_DIR / "*.csv").as_posix()

# -----------------------------
# OUTPUT + METRICS
# -----------------------------
OUT = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\Silver\DuckDB\10GB")
OUT.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\results\Duck_DB\10GB")
DATASET_NAME = "nyc_yellow_10gb"

def main():
    run_id = str(uuid.uuid4())
    t0 = time.time()

    con = duckdb.connect(database=":memory:")

    # ---- Ingest (read all csv) ----
    t_ing0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE VIEW taxi_raw AS
        SELECT *
        FROM read_csv_auto('{PATTERN}',
            IGNORE_ERRORS:=TRUE,
            SAMPLE_SIZE:=200000
        );
    """)
    # Force evaluation (avoid "lazy" surprises)
    _ = con.execute("SELECT COUNT(*) FROM taxi_raw;").fetchone()[0]
    t_ing = time.time() - t_ing0

    # ---- Clean/Cast ----
    t_c0 = time.time()
    con.execute("""
        CREATE OR REPLACE VIEW taxi_clean AS
        SELECT
          try_cast(tpep_pickup_datetime  AS TIMESTAMP) AS pickup_ts,
          try_cast(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
          try_cast(trip_distance AS DOUBLE)            AS trip_distance,
          try_cast(total_amount  AS DOUBLE)            AS total_amount
        FROM taxi_raw
        WHERE try_cast(total_amount AS DOUBLE) IS NOT NULL
          AND try_cast(total_amount AS DOUBLE) >= 0
          AND try_cast(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL
    """)
    _ = con.execute("SELECT COUNT(*) FROM taxi_clean;").fetchone()[0]
    t_clean = time.time() - t_c0

    # ---- Aggregation daily ----
    t_a0 = time.time()
    con.execute("""
        CREATE OR REPLACE VIEW taxi_daily AS
        SELECT
          date_trunc('day', pickup_ts) AS day,
          count(*)                     AS trips,
          avg(trip_distance)           AS avg_distance,
          sum(total_amount)            AS revenue
        FROM taxi_clean
        GROUP BY 1
        ORDER BY 1
    """)
    _ = con.execute("SELECT COUNT(*) FROM taxi_daily;").fetchone()[0]
    t_agg = time.time() - t_a0

    # ---- Export Parquet partition y/m ----
    t_e0 = time.time()
    con.execute(f"""
        COPY (
          SELECT day, trips, avg_distance, revenue,
                 strftime(day,'%Y') AS y,
                 strftime(day,'%m') AS m
          FROM taxi_daily
        )
        TO '{OUT.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (y,m), COMPRESSION 'SNAPPY', OVERWRITE 1);
    """)
    t_export = time.time() - t_e0

    latency = time.time() - t0

    # ---- Volume + throughput ----
    bytes_in = sum(p.stat().st_size for p in DATA_DIR.glob("*.csv"))
    volume_gb_disk = bytes_in / 1e9
    throughput_gb_min = volume_gb_disk / (latency / 60.0)

    metrics = {
        "run_id": run_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "stack": "duckdb",
        "dataset": DATASET_NAME,
        "volume_gb_disk": round(volume_gb_disk, 3),
        "t_ingest_s": round(t_ing, 3),
        "t_clean_s": round(t_clean, 3),
        "t_agg_s": round(t_agg, 3),
        "t_export_s": round(t_export, 3),
        "latency_total_s": round(latency, 3),
        "throughput_gb_min": round(throughput_gb_min, 3),
        "ok": True
    }

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "DuckDB_10GB_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"[OK] DuckDB 10GB done | metrics={out_dir/'metrics.json'} | total={metrics['latency_total_s']}s")

if __name__ == "__main__":
    print("[RUN] DuckDB 10GB")
    print("[DATA_DIR]", DATA_DIR, "exists?", DATA_DIR.exists())
    main()
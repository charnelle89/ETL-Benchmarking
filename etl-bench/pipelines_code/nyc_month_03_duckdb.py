import time, json, uuid
from pathlib import Path
from datetime import datetime
import duckdb
import os

CSV = Path(r"etl-bench/data/raw/1GB/yellow_tripdata_2016-03.csv").resolve()
OUT = Path("etl-bench/data/Silver/DuckDB"); OUT.mkdir(parents=True, exist_ok=True)

def main():
    t0 = time.time()
    con = duckdb.connect(database=":memory:")
    threads = os.cpu_count() or 4
    con.execute(f"pragma threads={threads};")

    # Ingest
    t_ing0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE VIEW taxi_raw AS
        SELECT * FROM read_csv_auto('{CSV.as_posix()}',
            IGNORE_ERRORS:=TRUE, SAMPLE_SIZE:=-1);
    """)
    t_ing = time.time() - t_ing0

    # Clean/Cast
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
    """)
    t_clean = time.time() - t_c0

    # Aggregations (par jour)
    t_a0 = time.time()
    con.execute("""
        CREATE OR REPLACE VIEW taxi_daily AS
        SELECT
          date_trunc('day', pickup_ts) AS day,
          count(*)                     AS trips,
          avg(trip_distance)           AS avg_distance,
          sum(total_amount)            AS revenue
        FROM taxi_clean
        WHERE pickup_ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """)
    t_agg = time.time() - t_a0

    # Export Parquet (partition y,m)
    t_e0 = time.time()
    con.execute(f"""
        COPY (
          SELECT day, trips, avg_distance, revenue,
                 strftime(day,'%Y') AS y, strftime(day,'%m') AS m
          FROM taxi_daily
        )
        TO '{OUT.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (y,m), COMPRESSION 'SNAPPY', OVERWRITE 1);
    """)
    t_export = time.time() - t_e0

    # Metrics
    latency = time.time() - t0
    run_id = str(uuid.uuid4())
    m = {
      "run_id": run_id,
      "timestamp": datetime.utcnow().isoformat()+"Z",
      "stack": "duckdb",
      "dataset": "nyc_yellow_2016_03",
      "volume_gb_disk": None,          # tu remplis juste après (step 3)
      "t_ingest_s": round(t_ing,3),
      "t_clean_s": round(t_clean,3),
      "t_agg_s": round(t_agg,3),
      "t_export_s": round(t_export,3),
      "latency_total_s": round(latency,3),
      "ok": True
    }
    out_dir = Path(f"etl-bench/results/Duck_DB/{run_id}"); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir/"DuckDB_1GB_metrics.json").write_text(json.dumps(m, indent=2), encoding="utf-8")
    print(f"[OK] y/m partitions -> {OUT} | metrics -> {out_dir/'metrics.json'} | total={m['latency_total_s']}s")

if __name__ == "__main__":
    print("[RUN] DuckDB NYC month")
    print("[CSV]", CSV, "exists?", CSV.exists())
    main()

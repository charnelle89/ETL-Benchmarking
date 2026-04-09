import os, time, json, uuid
from pathlib import Path
from datetime import datetime, UTC

import psutil
import pandas as pd
from pyspark.sql import SparkSession, functions as F

DATA_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\raw\10GB")
FILES = [str(p) for p in DATA_DIR.glob("*.csv")]

OUT_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\Silver\spark\10GB")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\results\Spark\10GB")
DATASET_NAME = "nyc_yellow_10gb"

def peak_rss_mb():
    p = psutil.Process(os.getpid())
    rss = p.memory_info().rss
    for c in p.children(recursive=True):
        try:
            rss += c.memory_info().rss
        except Exception:
            pass
    return rss / (1024*1024)

def main():
    print("[RUN] Spark 10GB (Windows safe)")
    print("[DATA_DIR]", DATA_DIR, "exists?", DATA_DIR.exists())
    print("[FILES]", len(FILES))
    if not DATA_DIR.exists() or len(FILES) == 0:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    run_id = str(uuid.uuid4())
    t0 = time.time()
    rss0 = peak_rss_mb()

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("etl-bench-nyc-10gb-spark")
             .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---- Ingest ----
    t_ing0 = time.time()
    df = spark.read.option("header","true").csv(FILES)
    _ = df.count()  # force read
    t_ing = time.time() - t_ing0

    # ---- Clean/Cast ----
    t_c0 = time.time()
    df2 = (df
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("total_amount").isNotNull())
        .filter(F.col("total_amount") >= 0)
    )
    _ = df2.count()
    t_clean = time.time() - t_c0

    # ---- Aggregation ----
    t_a0 = time.time()
    daily = (df2
        .withColumn("day", F.date_trunc("day", F.col("pickup_ts")))
        .groupBy("day")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("revenue"),
        )
        .orderBy("day")
        .withColumn("y", F.date_format("day", "yyyy"))
        .withColumn("m", F.date_format("day", "MM"))
    )
    _ = daily.count()
    t_agg = time.time() - t_a0

    # ---- Export (Windows workaround) ----
    t_e0 = time.time()
    pdf = daily.toPandas()
    out_parquet = OUT_DIR / f"{DATASET_NAME}_spark_daily.parquet"
    pdf.to_parquet(out_parquet, index=False)
    t_export = time.time() - t_e0

    latency = time.time() - t0
    rss_peak = max(rss0, peak_rss_mb())

    # volume disque = somme des fichiers
    bytes_in = sum(Path(f).stat().st_size for f in FILES)
    volume_gb_disk = bytes_in / 1e9
    throughput_gb_min = volume_gb_disk / (latency / 60.0)

    metrics = {
        "run_id": run_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "stack": "spark",
        "dataset": DATASET_NAME,
        "volume_gb_disk": round(volume_gb_disk, 3),
        "t_ingest_s": round(t_ing, 3),
        "t_clean_s": round(t_clean, 3),
        "t_agg_s": round(t_agg, 3),
        "t_export_s": round(t_export, 3),
        "latency_total_s": round(latency, 3),
        "throughput_gb_min": round(throughput_gb_min, 3),
        "peak_rss_mb": round(rss_peak, 1),
        "ok": True,
        "notes": "Windows workaround: no wildcard glob for input; write via pandas/pyarrow."
    }

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "Spark_10GB_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"[OK] Spark 10GB done | wrote={out_parquet} | metrics={out_dir/'metrics.json'} | total={metrics['latency_total_s']}s")
    spark.stop()

if __name__ == "__main__":
    main()
import os
import time
import json
import uuid
from pathlib import Path
from datetime import datetime, UTC

import psutil
import pandas as pd
from pyspark.sql import SparkSession, functions as F

# -----------------------------
# Paths (ADAPTE SI BESOIN)
# -----------------------------
CSV = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\raw\1GB\yellow_tripdata_2016-03.csv")
OUT_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\data\silver\spark\1GB")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\results\Spark\1GB")

DATASET_NAME = "nyc_yellow_2016_03_Spark"


def peak_rss_mb() -> float:
    """Approx peak memory for current process + children (Spark spawns subprocess/JVM)."""
    p = psutil.Process(os.getpid())
    rss = p.memory_info().rss
    for c in p.children(recursive=True):
        try:
            rss += c.memory_info().rss
        except Exception:
            pass
    return rss / (1024 * 1024)


def main():
    print("[RUN] Spark NYC month (Windows-safe write)")
    print("[CSV]", CSV, "exists?", CSV.exists())
    if not CSV.exists():
        raise FileNotFoundError(str(CSV))

    run_id = str(uuid.uuid4())
    t0 = time.time()
    rss_start = peak_rss_mb()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("etl-bench-nyc-spark")
        # Warehouse dir juste pour éviter des soucis de chemins sur Windows
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------
    # Ingest (Read CSV)
    # -----------------------------
    t_ing0 = time.time()
    df = (
        spark.read
        .option("header", "true")
        .csv(str(CSV))
    )
    t_ing = time.time() - t_ing0

    # -----------------------------
    # Clean / Cast
    # -----------------------------
    t_clean0 = time.time()
    df2 = (
        df
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("total_amount").isNotNull())
        .filter(F.col("total_amount") >= 0)
    )
    # petite action pour matérialiser le plan (optionnel mais utile)
    _ = df2.select(F.count(F.lit(1))).collect()
    t_clean = time.time() - t_clean0

    # -----------------------------
    # Aggregation (Daily KPIs)
    # -----------------------------
    t_agg0 = time.time()
    daily = (
        df2
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
    # force exécution de l'agg
    _ = daily.count()
    t_agg = time.time() - t_agg0

    # -----------------------------
    # Export (Windows workaround)
    # Spark write parquet sur Windows peut échouer (NativeIO winutils).
    # On collecte l'agg (petit) et on écrit via pandas/pyarrow.
    # -----------------------------
    t_exp0 = time.time()
    pdf = daily.toPandas()
    out_parquet = OUT_DIR / f"{DATASET_NAME}_spark_daily.parquet"
    pdf.to_parquet(out_parquet, index=False)  # nécessite pyarrow installé
    t_export = time.time() - t_exp0

    # -----------------------------
    # Metrics
    # -----------------------------
    latency = time.time() - t0
    rss_end = peak_rss_mb()
    peak_rss = max(rss_start, rss_end)

    bytes_in = CSV.stat().st_size
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
        "peak_rss_mb": round(peak_rss, 1),
        "out_parquet": str(out_parquet),
        "ok": True,
        "notes": "Windows workaround: Spark write->pandas/pyarrow (avoid Hadoop NativeIO winutils issue)."
    }

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "Spark_1GB_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"[OK] wrote parquet -> {out_parquet}")
    print(f"[OK] metrics -> {out_dir / 'Spark_1GB_metrics.json'}")
    print(f"[OK] total={metrics['latency_total_s']}s throughput={metrics['throughput_gb_min']} GB/min peak_rss={metrics['peak_rss_mb']} MB")

    spark.stop()


if __name__ == "__main__":
    main()
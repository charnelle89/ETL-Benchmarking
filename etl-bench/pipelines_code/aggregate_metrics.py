import json, glob, os
import pandas as pd

# 1) Charger tous les JSON dans etl-bench/results (peu importe le nom)
paths = glob.glob("etl-bench/results/**/*.json", recursive=True)

rows = []
for p in paths:
    try:
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)

        # On garde uniquement les "metrics" (dict avec au moins stack + latency)
        if not isinstance(d, dict):
            continue
        if "stack" not in d:
            continue
        if "latency_total_s" not in d and "latency_sec" not in d:
            continue

        # normaliser ancien champ
        if "latency_total_s" not in d and "latency_sec" in d:
            d["latency_total_s"] = d["latency_sec"]

        d["_file"] = p.replace("\\", "/")
        rows.append(d)

    except Exception:
        pass

df = pd.DataFrame(rows)
if df.empty:
    print("Aucun metrics JSON détecté sous etl-bench/results/")
    raise SystemExit(1)

# 2) Normaliser le "dataset_group" => 1GB vs 10GB
def dataset_group(row):
    ds = str(row.get("dataset", "")).lower()
    vol = row.get("volume_gb_disk", None)

    # règle 1 : si le nom contient 10gb
    if "10gb" in ds:
        return "10gb"

    # règle 2 : si volume est disponible et >= 8GB => 10gb (robuste)
    try:
        if vol is not None and float(vol) >= 8.0:
            return "10gb"
    except Exception:
        pass

    # sinon => 1gb
    return "1gb"

df["dataset_group"] = df.apply(dataset_group, axis=1)

# 3) Garder colonnes utiles (si elles existent)
cols = [
    "dataset_group", "stack", "dataset", "volume_gb_disk",
    "t_ingest_s", "t_clean_s", "t_agg_s", "t_export_s",
    "latency_total_s", "throughput_gb_min", "peak_rss_mb",
    "_file"
]
cols = [c for c in cols if c in df.columns]
df = df[cols].sort_values(["dataset_group", "stack", "latency_total_s"])

# 4) Écrire outputs
os.makedirs("etl-bench/results/aggregates", exist_ok=True)

out_all = "etl-bench/results/aggregates/summary_all.csv"
df.to_csv(out_all, index=False)
print("[OK] wrote", out_all)

out_1 = "etl-bench/results/aggregates/summary_1gb.csv"
df[df["dataset_group"] == "1gb"].to_csv(out_1, index=False)
print("[OK] wrote", out_1)

out_10 = "etl-bench/results/aggregates/summary_10gb.csv"
df[df["dataset_group"] == "10gb"].to_csv(out_10, index=False)
print("[OK] wrote", out_10)

# 5) Résumé mean/min/max par groupe & stack (très utile pour ton rapport)
num_cols = [c for c in df.columns if c not in ("dataset_group", "stack", "dataset", "_file")]
summary = df.groupby(["dataset_group", "stack"])[num_cols].agg(["mean", "min", "max"])
out_sum = "etl-bench/results/aggregates/summary_stats.csv"
summary.to_csv(out_sum)
print("[OK] wrote", out_sum)

print("\n--- PREVIEW (top 15) ---")
print(df.head(15).to_string(index=False))
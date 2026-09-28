import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re

# -----------------------------
# CONFIGURATION DES CHEMINS
# -----------------------------
RESULTS_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\results")
PLOTS_DIR = Path(r"C:\Users\charn\Desktop\Master\Forschungsprojekt\etl-bench\plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def load_all_metrics():
    """Parcourt tous les fichiers JSON et extrait les métriques."""
    data = []
    # Cherche tous les fichiers json dans les sous-dossiers
    for json_file in RESULTS_DIR.rglob("*.json"):
        with open(json_file, 'r', encoding='utf-8') as f:
            try:
                metrics = json.load(f)
                if metrics.get("ok"):
                     match = re.search(r"(\d+)GB", str(json_file))

                if not match:
                        print(f"Taille impossible à déterminer pour {json_file}")
                        continue

                size_category = int(match.group(1))
                    # On arrondit le volume pour grouper facilement (1, 3, 5, 7, 10 GB)
                    
                   
                data.append({
                        "Engine": "DuckDB" if metrics["stack"].lower() == "duckdb" else "Spark",
                        "Size_GB": size_category,
                        "Latency_s": metrics["latency_total_s"],
                        "Peak_RAM_MB": metrics["peak_rss_mb"]
                    })
            except Exception as e:
                print(f"Erreur de lecture sur {json_file.name}: {e}")
    return pd.DataFrame(data)

def generate_plots_and_tables():
    df = load_all_metrics()
   
    if df.empty:
        print("Aucune donnée trouvée. Vérifie le chemin RESULTS_DIR.")
        return

    # Configuration du style académique
    sns.set_theme(style="whitegrid")
    plt.rcParams.update({'font.size': 12, 'figure.dpi': 300})

    # ==========================================
    # 1. GRAPHIQUE : LATENCE (TEMPS)
    # ==========================================
    plt.figure(figsize=(8, 5))
    sns.lineplot(
        data=df, x="Size_GB", y="Latency_s", hue="Engine",
        marker="o", err_style="bars", linewidth=2, markersize=8
    )
    plt.title("End-to-End ETL Latency vs Data Volume", fontweight='bold')
    plt.xlabel("Data Volume (GB)")
    plt.ylabel("Latency (Seconds)")
    plt.xticks([1, 3, 5, 7, 10]) # Force l'affichage des paliers
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "latency_comparison.png")
    plt.close()

    # ==========================================
    # 2. GRAPHIQUE : MÉMOIRE (PEAK RAM)
    # ==========================================
    plt.figure(figsize=(8, 5))
    sns.lineplot(
        data=df, x="Size_GB", y="Peak_RAM_MB", hue="Engine",
        marker="s", err_style="bars", linewidth=2, markersize=8, palette=["#FF7F0E", "#1F77B4"]
    )
    plt.title("Peak Memory Usage (RSS) vs Data Volume", fontweight='bold')
    plt.xlabel("Data Volume (GB)")
    plt.ylabel("Peak RAM (MB)")
    plt.xticks([1, 3, 5, 7, 10])
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "memory_comparison.png")
    plt.close()

    print(f"[OK] Graphiques générés dans : {PLOTS_DIR}\n")

    # ==========================================
    # 3. GÉNÉRATION DU CODE LATEX (TABLEAU)
    # ==========================================
    # Calcul des moyennes et écarts-types
    summary = df.groupby(["Engine", "Size_GB"]).agg(
        Latency_Mean=("Latency_s", "mean"),
        Latency_Std=("Latency_s", "std"),
        RAM_Mean=("Peak_RAM_MB", "mean"),
        RAM_Std=("Peak_RAM_MB", "std")
    ).reset_index()

    print("=== COPIE CE CODE DANS TON FICHIER LATEX ===")
    print("\\begin{table}[h!]")
    print("\\centering")
    print("\\begin{tabular}{|l|c|cc|cc|}")
    print("\\hline")
    print("\\textbf{Engine} & \\textbf{Size (GB)} & \\textbf{Latency (s)} & \\textbf{$\\pm$ Std} & \\textbf{Peak RAM (MB)} & \\textbf{$\\pm$ Std} \\\\")
    print("\\hline")
   
    for _, row in summary.iterrows():
        print(f"{row['Engine']} & {row['Size_GB']} & {row['Latency_Mean']:.2f} & {row['Latency_Std']:.2f} & {row['RAM_Mean']:.2f} & {row['RAM_Std']:.2f} \\\\")
   
    print("\\hline")
    print("\\end{tabular}")
    print("\\caption{Vergleich der mittleren Latenz und des Speicherverbrauchs (n=30 Läufe pro Stufe).}")
    print("\\label{tab:results_summary}")
    print("\\end{table}")
    print("============================================")

if __name__ == "__main__":
    generate_plots_and_tables()
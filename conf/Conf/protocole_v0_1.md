# Objectif

bjectif : pipeline identique (Ingest → Clean → Join → Agg → Export) exécutée sur Spark et DuckDB.
Volumes : ~1 Go, ~10 Go, ~100 Go. Répétitions : 3 par condition (stack × volume).
Mesures : timestamps début/fin par étape → latence E2E ; p95/p99 (définition) ; Go/min = volume / (latence/60).
Conditions : même machine, pas d’autres charges ; versions des outils listées en annexe.
Exactitude : les agrégations (NYC daily trips/avg_distance/revenue ; Amazon daily n_reviews/avg_rating/p_verified) doivent matcher entre stacks (tolérance d’arrondi).
Risques & parades : cache chaud/froid (ordre alterné), outliers, tailles de fichiers variables

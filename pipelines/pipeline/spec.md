# Spécification de Pipeline (ETL-Benchmark)

## NYC Taxi

### Colonnes utilisées
- `tpep_pickup_datetime` → `pickup_ts` (TIMESTAMP)
- `tpep_dropoff_datetime` → `dropoff_ts` (TIMESTAMP)
- `trip_distance` (DOUBLE)
- `total_amount` (DOUBLE)
- `PULocationID` (INT)
- `DOLocationID` (INT)

### Étapes
1) **Ingest** : lire les CSV mensuels sélectionnés (chemin: `data/raw/nyc_taxi/<service>/YYYY/MM/*.csv`)
2) **Clean/Cast** : 
   - caster `pickup_ts`, `dropoff_ts`, `trip_distance`, `total_amount`
   - filtre : `total_amount >= 0`
   - drop NA sur colonnes clés
3) **Join/Enrich** : `taxi_zone_lookup.csv` (clé `LocationID` → `borough`, `zone`)
4) **Aggregations** (par **jour**) :
   - `trips = count(*)`
   - `avg_distance = avg(trip_distance)`
   - `revenue = sum(total_amount)`
5) **Export** : Parquet partitionné par `y`, `m` (issus de `pickup_ts`)

### Oracle d’exactitude (NYC)
- Les résultats (lignes/valeurs) doivent être identiques entre stacks à ±1e-6 sur les agrégations.

---

## Amazon Reviews

### Colonnes utilisées
- `review_date` (DATE)
- `product_category` (STRING)
- `star_rating` (INT ∈ {1,2,3,4,5})
- `verified_purchase` (STRING ∈ {Y,N})
- `helpful_votes` (INT, optionnel)
- `product_id` (STRING, optionnel pour join)

### Étapes
1) **Ingest** : lire partitions par `(category, year)` depuis `data/raw/amazon/<cat>/YYYY/*`
2) **Clean/Cast** :
   - caster `review_date`
   - filtre : `star_rating ∈ [1..5]`, `verified_purchase ∈ {Y,N}`
3) **Aggregations** (par **jour** et **catégorie**) :
   - `n_reviews = count(*)`
   - `avg_rating = avg(star_rating)`
   - `p_verified = avg(CASE verified_purchase='Y' THEN 1 ELSE 0 END)`
4) **Export** : Parquet partitionné par `y`, `m`, `category`

### Oracle d’exactitude (Amazon)
- Même nombre de lignes et mêmes valeurs agrégées entre stacks (tolérance d’arrondi ±1e-6).

---

## Contraintes communes
- **Pipeline identique** (mêmes étapes et mêmes agrégations) pour chaque stack.
- **3 répétitions** par condition (stack × volume).
- **Mesures par étape** : timestamps début/fin → latence par étape + latence E2E.
- **Débit (Go/min)** = volume_approx_Go / (latence_E2E_sec / 60).
- **Quantiles** : p95/p99 calculés sur les durées si sous-étapes disponibles (sinon sur répétitions).

## TODO (à compléter par moi)
- Lister les mois NYC et (catégorie, année) Amazon retenus pour 1/10/100 Go.
- Chemins exacts des fichiers bruts et de la table `taxi_zone_lookup`.
- Définir la tolérance d’arrondi exacte et les règles “cache froid/chaud”.

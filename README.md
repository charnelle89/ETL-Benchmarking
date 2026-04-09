# ETL Benchmark: DuckDB vs Apache Spark (Single-Machine Study)

## Overview

This project presents a reproducible ETL benchmark comparing DuckDB and Apache Spark (Local Mode) in a single-machine environment.

The objective is to evaluate performance behavior under increasing data volumes using a realistic ETL pipeline based on the NYC Taxi dataset.

The study focuses on:
- End-to-end latency
- Stability (standard deviation)
- Statistical significance (t-test, 95% confidence interval)
- Scaling behavior

---

## Research Question

How do DuckDB and Apache Spark compare in ETL workloads on a single machine, and how does performance evolve with increasing data size?

---

## ETL Pipeline Structure

The benchmark implements identical pipelines in both engines:

1. Extract  
   Load raw CSV files from NYC Taxi dataset.

2. Transform  
   - Type casting (timestamps, numeric fields)  
   - Data cleaning  
   - Filtering invalid records  

3. Aggregation  
   - Daily grouping  
   - COUNT(*)  
   - AVG(trip_distance)  
   - SUM(total_amount)  

4. Load  
   Write aggregated output as partitioned Parquet files.

---

## Experimental Setup

- Environment: Windows  
- Execution Mode: Single-Machine  
- Spark: Local Mode  
- Runs per configuration: 30  
- Data Sizes:
  - ~1 GB  
  - ~10 GB  

Metrics recorded:
- End-to-end runtime  
- Throughput  
- Standard deviation  
- 95% confidence interval  
- Independent t-test  

---

## Key Results

| Data Size | Faster Engine |
|------------|---------------|
| 1 GB       | Spark         |
| 10 GB      | DuckDB        |

Results show a reversal of dominance depending on data size.

Spark benefits from parallelism at small scale.  
DuckDB scales more efficiently at larger volumes in a single-machine setting.

---

## Dataset

The NYC Taxi dataset is not included in this repository due to size constraints.

It can be downloaded from:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## Project Structure

conf/  
pipelines/  
etl-bench/  
pipelines_code/  

---

## Reproducibility

Each configuration was executed 30 times.  
Statistical evaluation includes:
- Mean
- Standard deviation
- 95% confidence interval
- Independent t-test (α = 0.05)

---

## Conclusion

The benchmark demonstrates that tool selection for ETL workloads depends strongly on:

- Data size  
- Execution environment  
- Architectural overhead  

Distributed frameworks are not automatically superior in all contexts.

---

Master’s Research Project  
Applied Computer Science  
HTW Berlin

import pandas as pd

df = pd.read_csv(r"C:/Users/charn/Desktop/Master/Forschungsprojekt/etl-bench/results/aggregates/summary_1gb.csv")
print(df.head())

duckdb_1gb = df[df["stack"]=="duckdb"]["latency_total_s"].values
spark_1gb = df[df["stack"]=="spark"]["latency_total_s"].values

print(len(duckdb_1gb))  
print(len(spark_1gb))   
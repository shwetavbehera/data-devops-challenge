from pyspark.sql import SparkSession
from reader import DataReader
from cleaner import DataCleaner
from transformer import DataTransformer
from writer import DataWriter

spark = SparkSession.builder.master("local[*]").appName("LocalTest").getOrCreate()

source_path = "./data/financial"
output_path_1 = r"C:\Users\shwet\OneDrive\Documents\GitHub\data-devops-challenge\output\transactions_cleaned"
output_path_2 = r"C:\Users\shwet\OneDrive\Documents\GitHub\data-devops-challenge\output\loan_by_district"

reader = DataReader(spark, base_path=source_path)
dfs = reader.read_all()

cleaner = DataCleaner(dfs)
dfs_cleaned = cleaner.clean()

transformer = DataTransformer(dfs_cleaned)

writer = DataWriter()

#Check transformation results
# --- Transactions after cleaning ---
df_trans = transformer.cleaned_transactions()
print("\n=== Cleaned Transactions ===")
df_trans.printSchema()
print(f"Row count: {df_trans.count()}")
df_trans.show(5, truncate=False)

print(df_trans.select("type").distinct().show())
print(df_trans.filter("type = 'PRJIEM'").count())

# --- Average loan per district ---
df_avg = transformer.avg_loan_per_district()
print("\n=== Avg Loan per District ===")
df_avg.printSchema()
df_avg.show(5, truncate=False)


# Write cleaned transactions
writer.write_parquet(transformer.cleaned_transactions(), output_path_1)

# Write average loan per district
writer.write_parquet(transformer.avg_loan_per_district(), output_path_2)


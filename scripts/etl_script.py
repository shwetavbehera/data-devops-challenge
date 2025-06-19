import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Local modules
from reader import DataReader
from cleaner import DataCleaner
from transformer import DataTransformer
from writer import DataWriter

# Glue arguments
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "destination_path"]
)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step 1: Read input
reader = DataReader(spark, base_path=args["source_path"])
dfs = reader.read_all()

# Step 2: Clean
cleaner = DataCleaner(dfs)
dfs_cleaned = cleaner.clean()

# Step 3: Transform
transformer = DataTransformer(dfs_cleaned)

# Step 4: Write output
writer = DataWriter()

# Write cleaned transactions to: <destination_path>/transactions_cleaned/
writer.write_parquet(
    transformer.cleaned_transactions(),
    f"{args['destination_path']}transactions_cleaned/"
)

# Write average loan per district to: <destination_path>/avg_loan_by_district/
writer.write_parquet(
    transformer.avg_loan_per_district(),
    f"{args['destination_path']}avg_loan_by_district/"
)

# Step 5: Commit job
job.commit()

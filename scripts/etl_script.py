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

# read input
reader = DataReader(spark, base_path=args["source_path"])
dfs = reader.read_all()

# clean
cleaner = DataCleaner(dfs)
dfs_cleaned = cleaner.clean()

# transform
transformer = DataTransformer(dfs_cleaned)

# write output
writer = DataWriter()

# write cleaned transactions to: <destination_path>/transactions_cleaned/
writer.write_parquet(
    transformer.cleaned_transactions(),
    f"{args['destination_path']}transactions_cleaned/"
)

# write average loan per district to: <destination_path>/avg_loan_by_district/
writer.write_parquet(
    transformer.avg_loan_per_district(),
    f"{args['destination_path']}avg_loan_by_district/"
)

# commit job
job.commit()

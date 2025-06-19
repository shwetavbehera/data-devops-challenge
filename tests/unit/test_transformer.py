import pytest
from pyspark.sql import SparkSession
from scripts.transformer import DataTransformer

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

def test_avg_loan_per_district(spark):
    loan_df = spark.createDataFrame([
        (1, 1000.0),
        (2, 2000.0)
    ], ["account_id", "amount"])

    account_df = spark.createDataFrame([
        (1, 10),
        (2, 10)
    ], ["account_id", "district_id"])

    dfs = {"loan": loan_df, "account": account_df}
    transformer = DataTransformer(dfs)
    result = transformer.avg_loan_per_district().collect()

    assert result[0]["district_id"] == 10
    assert result[0]["avg_loan_amount"] == 1500.0

import pytest
from pyspark.sql import SparkSession
from scripts.cleaner import DataCleaner

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("unit-test").getOrCreate()

def test_clean_typo_and_join(spark):
    trans_df = spark.createDataFrame([
        (1, "PRJIEM"),
        (2, "VYDAJ")
    ], ["account_id", "type"])

    loan_df = spark.createDataFrame([
        (1,), (3,)
    ], ["account_id"])

    cleaner = DataCleaner({"trans": trans_df, "loan": loan_df})
    cleaned = cleaner.clean()
    result = cleaned["trans"].collect()

    assert any(row["type"] == "PRIJEM" for row in result)
    assert all(row["account_id"] != 2 for row in result)

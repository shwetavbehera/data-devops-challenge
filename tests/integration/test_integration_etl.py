import pytest
from pyspark.sql import SparkSession
from reader import DataReader
from cleaner import DataCleaner
from transformer import DataTransformer

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("integration-test").getOrCreate()

def test_etl_pipeline(spark, tmp_path):
    # Setup dummy CSV files
    source_path = tmp_path / "input"
    source_path.mkdir()

    # Create small CSVs for account, loan, trans
    (source_path / "account.csv").write_text("account_id;district_id\n1;77\n2;88")
    (source_path / "loan.csv").write_text("account_id;amount\n1;1000\n2;2000")
    (source_path / "trans.csv").write_text("account_id;type\n1;PRJIEM\n2;VYDAJ")

    reader = DataReader(spark, str(source_path))
    dfs = reader.read_all()

    cleaner = DataCleaner(dfs)
    cleaned_dfs = cleaner.clean()

    transformer = DataTransformer(cleaned_dfs)
    avg_loan_df = transformer.avg_loan_per_district()

    result = avg_loan_df.collect()
    assert result[0]["district_id"] == 77

import pytest
from pyspark.sql import SparkSession
from scripts.reader import DataReader
from scripts.cleaner import DataCleaner
from scripts.transformer import DataTransformer

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

    # so Spark can infer a schema
    stubs = {
        "card": "card_id;disp_id;type\n",
        "client": "client_id;birth_number;district_id\n",
        "disp": "disp_id;client_id;account_id;type\n",
        "district": "district_id;name\n",
        "order": "order_id;account_id;bank_to;account_to;amount;k_symbol\n",
    }
    for name, content in stubs.items():
        (source_path / f"{name}.csv").write_text(content)

    reader = DataReader(spark, str(source_path))
    dfs = reader.read_all()

    cleaner = DataCleaner(dfs)
    cleaned_dfs = cleaner.clean()

    transformer = DataTransformer(cleaned_dfs)
    avg_loan_df = transformer.avg_loan_per_district()

    rows = avg_loan_df.collect()
    rows_by_id = {r["district_id"]: r["avg_loan_amount"] for r in rows}
    assert rows_by_id == {77: 1000.0, 88: 2000.0}

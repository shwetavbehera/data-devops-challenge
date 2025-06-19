import pytest
from unittest.mock import MagicMock
from scripts.reader import DataReader

def test_read_all_calls_read_correctly():
    spark = MagicMock()
    reader = DataReader(spark, "/some/path")
    reader._read = MagicMock()
    result = reader.read_all()
    
    assert reader._read.call_count == 8
    expected_tables = ["account", "card", "client", "disp", "district", "loan", "order", "trans"]
    assert sorted(result.keys()) == sorted(expected_tables)

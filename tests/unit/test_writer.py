from unittest.mock import MagicMock
from scripts.writer import DataWriter

def test_write_parquet_called():
    df_mock = MagicMock()
    writer = DataWriter()
    writer.write_parquet(df_mock, "some/path")

    df_mock.write.mode.assert_called_with("overwrite")
    df_mock.write.mode().parquet.assert_called_with("some/path")

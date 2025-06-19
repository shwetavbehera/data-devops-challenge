from pyspark.sql import DataFrame
from typing import Dict


class DataWriter:
    def __init__(self, output_base_path: str):
        self.output_base_path = output_base_path.rstrip("/")

    def write_parquet(self, name: str, df: DataFrame):
        """
        Writes a DataFrame to a Parquet file.
        :param name: Name of the DataFrame, used to create the output path
        :param df: DataFrame to write
        """
        path = f"{self.output_base_path}/{name}"
        df.write.mode("overwrite").parquet(path)

    def write_all(self, dfs: Dict[str, DataFrame], names: list = None):
        """
        Writes all DataFrames in the provided dictionary to Parquet files.
        :param dfs: Dictionary of DataFrames to write
        :param names: Optional list of DataFrame names to write. If None, all DataFrames are written.
        """
        for name, df in dfs.items():
            if names is None or name in names:
                self.write_parquet(name, df)

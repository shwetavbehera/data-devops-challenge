from pyspark.sql import SparkSession, DataFrame
from typing import Dict


class DataReader:
    def __init__(self, spark: SparkSession, base_path: str, file_format: str = "csv"):
        """
        Initialize the DataReader.

        :param spark: An existing SparkSession
        :param base_path: Base path where CSV files are located (can be local or S3)
        :param file_format: File format to read (default = "csv")
        """
        self.spark = spark
        self.base_path = base_path.rstrip("/")  # remove trailing slash
        self.file_format = file_format

    def _read_csv(self, name: str):
        path = f"{self.base_path}/{name}.csv"
        return self.spark.read.format(self.file_format).options(
            header=True, sep=";", inferSchema=True
        ).load(path)

    def read_all(self) -> Dict[str, DataFrame]:
        """
        Reads all expected CSV files and returns them as a dictionary of DataFrames.
        """
        return {
            "account": self._read_csv("account"),
            "card": self._read_csv("card"),
            "client": self._read_csv("client"),
            "disp": self._read_csv("disp"),
            "district": self._read_csv("district"),
            "loan": self._read_csv("loan"),
            "order": self._read_csv("order"),
            "trans": self._read_csv("trans"),
        }

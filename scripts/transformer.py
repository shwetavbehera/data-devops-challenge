from pyspark.sql import DataFrame
from typing import Dict


class DataTransformer:
    def __init__(self, dataframes: Dict[str, DataFrame]):
        self.dfs = dataframes

    def avg_loan_amount_per_district(self) -> DataFrame:
        """
        Computes the average loan amount per district.
        """
        joined = self.dfs["loan"] \
            .join(self.dfs["account"], on="account_id") \
            .join(self.dfs["client"], on="client_id") \
            .join(self.dfs["district"], on="district_id")

        return joined.groupBy("district_id", "A2").avg("amount") \
            .withColumnRenamed("avg(amount)", "avg_loan_amount") \
            .withColumnRenamed("A2", "district_name")

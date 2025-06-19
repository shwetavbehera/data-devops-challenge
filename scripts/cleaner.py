from pyspark.sql import DataFrame
from typing import Dict


class DataCleaner:
    def __init__(self, dataframes: Dict[str, DataFrame]):
        self.dfs = dataframes

    def fix_trans_typo(self) -> DataFrame:
        """
        Corrects the typo 'PRJIEM' to 'PRIJEM' in the 'type' column of the 'trans' table.
        """
        return self.dfs["trans"].withColumn(
            "type",
            self.dfs["trans"]["type"].replace("PRJIEM", "PRIJEM")
        )

    def filter_invalid_transactions(self) -> DataFrame:
        """
        Removes transactions with an account_id not present in the contract table (order).
        """
        valid_ids = self.dfs["order"].select("account_id").distinct()
        return self.dfs["trans"].join(valid_ids, on="account_id", how="inner")

    def clean(self) -> Dict[str, DataFrame]:
        """
        Applies all cleaning steps and returns updated DataFrames.
        """
        trans_cleaned = self.fix_trans_typo()
        self.dfs["trans"] = trans_cleaned

        trans_filtered = self.filter_invalid_transactions()
        self.dfs["trans"] = trans_filtered

        return self.dfs
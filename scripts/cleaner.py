from pyspark.sql import functions as F

class DataCleaner:
    """
    * Fix typo PRJIEM → PRIJEM in trans.type
    * Keep only transactions whose `account_id` occurs in the *loan*
      table (=> interpreted here as the ‘contract’ table).
    """
    def __init__(self, dfs: dict[str, "DataFrame"]):
        self.dfs = dfs

    def clean(self) -> dict[str, "DataFrame"]:
        trans   = self.dfs["trans"]
        loans   = self.dfs["loan"]

        # a) correct typo
        trans = trans.withColumn(
            "type",
            F.when(F.col("type") == "PRJIEM", "PRIJEM").otherwise(F.col("type"))
        )

        # b) filter invalid account_id
        valid_accounts = loans.select("account_id").distinct()
        trans = (trans
                 .join(valid_accounts, on="account_id", how="inner")
                 .dropDuplicates())

        # return the updated dict
        cleaned = self.dfs.copy()
        cleaned["trans"] = trans
        return cleaned

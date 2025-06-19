from pyspark.sql import functions as F

class DataTransformer:
    """
    • Expose the cleaned ‘trans’ DF for writing
    • Produce a district-level aggregated loan table
    """
    def __init__(self, dfs: dict[str, "DataFrame"]):
        self.dfs = dfs

    # ---------- 1. “Main result” – cleaned transactions -------------
    def cleaned_transactions(self):
        return self.dfs["trans"]

    # ---------- 2. “Bonus” – avg loan per district -----------------
    def avg_loan_per_district(self):
        loan     = self.dfs["loan"]
        account  = self.dfs["account"]

        # join loan->account to get district_id
        enriched = (loan
                    .join(account.select("account_id", "district_id"),
                          on="account_id", how="left"))

        return (enriched
                .groupBy("district_id")
                .agg(F.avg("amount").alias("avg_loan_amount"))
                .orderBy("district_id"))

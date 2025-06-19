class DataReader:
    def __init__(self, spark, base_path: str):
        self.spark = spark
        self.base_path = base_path

    # Reads a CSV file
    def _read(self, name: str):
        return (self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("sep", ";")
                .csv(f"{self.base_path}/{name}.csv"))

    # Reads all tables and returns a dictionary of DataFrames
    def read_all(self) -> dict[str, "DataFrame"]:
        tables = ["account", "card", "client", "disp",
                  "district", "loan", "order", "trans"]
        return {t: self._read(t) for t in tables}

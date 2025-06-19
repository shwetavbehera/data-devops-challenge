class DataWriter:
    def __init__(self, mode: str = "overwrite"):
        self.mode = mode

    def write_parquet(self, df, path: str):
        (df.write
           .mode(self.mode)
           .parquet(path))

import os
import zipfile
import gdown
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataDownloader:
    def __init__(self, file_id: str, output: str):
        self.file_id = file_id
        self.output = output

    def download(self):
        gdown.download(f"https://drive.google.com/uc?id={self.file_id}", self.output, quiet=False)

    def extract(self, extract_to: str):
        with zipfile.ZipFile(self.output, 'r') as zip_ref:
            zip_ref.extractall(extract_to)


if __name__ == "__main__":
    file_id = os.getenv("FILE_ID")
    output = os.getenv("OUTPUT_ZIP")
    extract_to = os.getenv("EXTRACT_DIR")

    downloader = DataDownloader(file_id, output)
    downloader.download()
    downloader.extract(extract_to)
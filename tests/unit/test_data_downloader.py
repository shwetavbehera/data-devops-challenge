import pytest
from src.data_downloader import DataDownloader

def test_initialization():
    downloader = DataDownloader("fake_id", "fake_output.zip")
    assert downloader.file_id == "fake_id"
    assert downloader.output == "fake_output.zip"
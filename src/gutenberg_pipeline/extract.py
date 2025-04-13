import os
import tarfile
import time
import logging
import zipfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from tqdm import tqdm

GUTENBERG_FEEDS_URL="https://www.gutenberg.org/cache/epub/feeds"
RDF_FILES="rdf-files.tar.zip"
RDF_FILE_PATH = Path("../../data/rdf_files.tar.zip")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_rdf_file(url, filename) -> None:
    """
    Download a file from a URL with support for resuming downloads.
    """
    total_size = int(requests.head(url).headers.get("Content-Length", 0))

    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    while True:
        try:
            downloaded_size = os.path.getsize(filename) if os.path.exists(filename) else 0

            if downloaded_size >= total_size:
                print(f"Download complete: {filename}")
                break

            headers = {"Range": f"bytes={downloaded_size}-"}
            with session.get(url, headers=headers, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                mode = 'ab' if downloaded_size else 'wb'
                with open(filename, mode) as f:
                    with tqdm(total=total_size, initial=downloaded_size, unit='B', unit_scale=True, desc=url) as pbar:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def extract_rdf_file(rdf_file: Path) -> None:
    extract_to = rdf_file.parent/"rdf_files"
    with zipfile.ZipFile(rdf_file, "r") as zip_ref:
        zip_ref.extractall(extract_to)

    for file in os.listdir(extract_to):
        if file.endswith('.tar'):
            tar_path = os.path.join(extract_to, file)

            with tarfile.open(tar_path, 'r') as tar_ref:
                tar_ref.extractall(extract_to)

            print(f"Extracted: {tar_path}")
            break
    else:
        print("No .tar file found inside the .zip")


def load_rdf():
    """
    Load RDF data from the Gutenberg Project.
    """

    logger.info("Downloading RDF file...")
    # download_rdf_file(f"{GUTENBERG_FEEDS_URL}/{RDF_FILES}", RDF_FILE_PATH)
    logger.info("Extract RDF file...")
    extract_rdf_file(RDF_FILE_PATH)

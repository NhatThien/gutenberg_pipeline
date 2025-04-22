import logging
import os
import tarfile
import time
import zipfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

logger = logging.getLogger(__name__)

def download_rdf_file(url: str, filename: Path) -> None:
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
    logger.info(f"Starting download: {filename}")
    while True:
        try:
            downloaded_size = os.path.getsize(filename) if os.path.exists(filename) else 0

            if downloaded_size >= total_size:
                logger.info(f"Download complete: {filename}")
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
            logger.warning(f"Request error: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def extract_tar_zip_file(zip_file: Path, directory_name: str) -> None:
    """
    Extract a .tar.zip file to a specified directory.
    :param directory_name:
    :param zip_file:
    :return:
    """
    extract_to = zip_file.parent/directory_name
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(extract_to)

    for file in os.listdir(extract_to):
        if file.endswith('.tar'):
            tar_path = os.path.join(extract_to, file)

            with tarfile.open(tar_path, 'r') as tar_ref:
                tar_ref.extractall(extract_to)

            logger.info(f"Extracted: {tar_path}")
            break
    else:
        logger.info("No .tar file found inside the .zip")

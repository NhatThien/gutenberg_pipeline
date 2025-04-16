import os
import tarfile
import time
import logging
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ElementTree
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from tqdm import tqdm

GUTENBERG_FEEDS_URL="https://www.gutenberg.org/cache/epub/feeds"
DATA_FOLDER = Path("../../data")
RDF_ZIP_FILE_NAME= "rdf-files.tar.zip"
RDF_ZIP_FILE_PATH = DATA_FOLDER/RDF_ZIP_FILE_NAME
RDF_UNZIP_FOLDER_NAME = "rdf_files"
RDF_UNZIP_FOLDER_PATH = DATA_FOLDER/RDF_UNZIP_FOLDER_NAME

NAMESPACES = {
    'pg': 'http://www.gutenberg.org/2009/pgterms/',
    'dc': 'http://purl.org/dc/terms/',
    'dcam': 'http://purl.org/dc/dcam/',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
}

logging.basicConfig(level=logging.INFO)
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
            logger.error(f"Request error: {e}. Retrying in 5 seconds...")
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


def load_rdf() -> None:
    """
    Load RDF data from the Gutenberg Project.
    """
    if not RDF_ZIP_FILE_PATH.exists():
        logger.info("Downloading RDF file...")
        download_rdf_file(f"{GUTENBERG_FEEDS_URL}/{RDF_ZIP_FILE_NAME}", RDF_ZIP_FILE_PATH)
    if not RDF_UNZIP_FOLDER_PATH.exists():
        logger.info("Extract RDF file...")
        extract_tar_zip_file(RDF_ZIP_FILE_PATH, RDF_UNZIP_FOLDER_NAME)


def parse_xml_file(file_path: Path) -> Optional[ElementTree.ElementTree]:
    """
    Parse an XML file and return the ElementTree object.
    :param file_path: Path to the XML file.
    :return: ElementTree object.
    """
    try:
        tree = ElementTree.parse(file_path)
        return tree
    except ElementTree.ParseError as e:
        logger.error(f"Error parsing XML file {file_path}: {e}")
        return None


def extract_metadata(xml_tree: ElementTree.ElementTree) -> dict:
    """
    Extract metadata from the XML tree.
    :param xml_tree:
    :return:
    """
    root = xml_tree.getroot()

    def extract_id(text: str) -> Optional[str]:
        try:
            print(text)
            return text.split("/")[-1] if text else None
        except ValueError as e:
            logger.error("Error extracting ID: {e}")
            return None

    def get_text(tag: str) -> Optional[str]:
        el = root.find(tag, NAMESPACES)
        return el.text if el is not None else None

    def get_text_list(tag: str) -> Optional[list[str]]:
        el = root.findall(tag, NAMESPACES)
        return [e.text for e in el] if el else None

    def get_book_link() -> Optional[str]:
        for file_elem in root.findall(".//pg:file", NAMESPACES):
            format_value = file_elem.find(".//dc:format/rdf:Description/rdf:value", NAMESPACES)
            if format_value is not None and format_value.text.startswith("text/plain"):
                return file_elem.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")

        return None

    def get_book_id() -> Optional[int]:
        el = root.find('.//pg:ebook', NAMESPACES)
        if el is not None:
            return parse_int(extract_id(el.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")))

        return None

    def parse_int(text: Optional[str]) -> Optional[int]:
        try:
            return int(text) if text else None
        except (ValueError, TypeError):
            return None

    def get_nested_text(el: ElementTree.Element, tag: str) -> Optional[str]:
        child = el.find(tag, NAMESPACES)
        return child.text if child is not None else None

    def get_authors() -> Optional[list[dict]]:
        authors = root.findall('.//dc:creator', NAMESPACES)
        if not authors:
            return None
        res = []
        for author in authors:
            agent = author.find('.//pg:agent', NAMESPACES)
            author_id = extract_id(agent.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"))
            author_name = get_nested_text(author, './/pg:name')
            author_birth_year =  parse_int(get_nested_text(author, './/pg:birthdate'))
            author_death_year =  parse_int(get_nested_text(author,'.//pg:deathdate'))
            res.append(
                {
                    "id": author_id,
                    "name": author_name,
                    "birth_year": author_birth_year,
                    "death_year": author_death_year
                }
            )

        return res if res else None

    return {
        "gutenberg_id": get_book_id(),
        "type": get_text('.//dc:type/rdf:value'),
        "authors": get_authors(),
        "title": get_text('.//dc:title'),
        "summary": get_text('.//pg:marc520'),
        "language": get_text_list('.//dc:language/rdf:Description/rdf:value'),
        "book_link": get_book_link(),
    }

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp
import typer

from gutenberg_pipeline.database import Session
from gutenberg_pipeline.extract.downloader import download_rdf_file, extract_tar_zip_file
from gutenberg_pipeline.transfer import store_book_to_db
from gutenberg_pipeline.config import Config

GUTENBERG_FEEDS_URL = "https://www.gutenberg.org/cache/epub/feeds"
RDF_ZIP_FILE_NAME = "rdf-files.tar.zip"
RDF_ZIP_FILE_PATH = Config.DATA_FOLDER / RDF_ZIP_FILE_NAME
RDF_UNZIP_FOLDER_NAME = "rdf_files"
RDF_UNZIP_FOLDER_PATH = Config.DATA_FOLDER / RDF_UNZIP_FOLDER_NAME

BATCH_SIZE = 100
MAX_WORKERS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()

@app.command()
def load(limit: Optional[int]=BATCH_SIZE) -> None:
    asyncio.run(main(limit))

def prepare_rdf_data() -> None:
    """
    Ensure RDF data is downloaded and extracted.
    """
    if not RDF_ZIP_FILE_PATH.exists():
        logger.info("RDF archive not found. Downloading...")
        download_rdf_file(f"{GUTENBERG_FEEDS_URL}/{RDF_ZIP_FILE_NAME}", RDF_ZIP_FILE_PATH)
    else:
        logger.info("RDF archive already exists. Skipping download.")

    if not RDF_UNZIP_FOLDER_PATH.exists():
        logger.info("Extracting RDF archive...")
        extract_tar_zip_file(RDF_ZIP_FILE_PATH, RDF_UNZIP_FOLDER_NAME)
    else:
        logger.info("RDF folder already extracted. Skipping extraction.")

def process_rdf_files(limit: int) -> list[Path]:
    """
    Process RDF files and store book metadata into the database.
    :param limit: Maximum number of files to process.
    """
    folder = RDF_UNZIP_FOLDER_PATH / "cache/epub"
    count = 0

    metadata_books_to_index = []
    for sub_folder in folder.iterdir():
        if not sub_folder.is_dir():
            continue

        for rdf_file in sub_folder.glob("*.rdf"):
            logger.info(f"Found RDF file: {rdf_file}")
            metadata_books_to_index.append(rdf_file)
        count += 1
        if count >= limit:
            logger.info(f"Processing limit ({limit}) reached. Stopping.")
            break

    return metadata_books_to_index

async def main(limit: int) -> None:
    """
    Main entry point to prepare RDF data and process it.
    """
    prepare_rdf_data()
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    start_time = time.time()
    async with aiohttp.ClientSession() as http_session:
        with Session() as db_session:
            books = process_rdf_files(limit)
            tasks = [store_book_to_db(semaphore, http_session, db_session, book_metadata) for book_metadata in books]
            results = await asyncio.gather(*tasks)

    logger.info(f"Processed {len(results)} books in {time.time() - start_time:.2f} seconds.")
    for idx, book in enumerate(results):
        if book:
            logger.info(f"Task {idx}: Book '{book.title}' stored successfully.")
        else:
            logger.warning(f"Task {idx}: Failed to store book.")

if __name__ == "__main__":
    app()

import logging

from gutenberg_pipeline.database import Session
from gutenberg_pipeline.extract.downloader import download_rdf_file, extract_tar_zip_file
from gutenberg_pipeline.transfer import store_book_to_db
from gutenberg_pipeline.config import Config

GUTENBERG_FEEDS_URL = "https://www.gutenberg.org/cache/epub/feeds"
RDF_ZIP_FILE_NAME = "rdf-files.tar.zip"
RDF_ZIP_FILE_PATH = Config.DATA_FOLDER / RDF_ZIP_FILE_NAME
RDF_UNZIP_FOLDER_NAME = "rdf_files"
RDF_UNZIP_FOLDER_PATH = Config.DATA_FOLDER / RDF_UNZIP_FOLDER_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

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

def process_rdf_files(session: Session, limit: int = 10) -> None:
    """
    Process RDF files and store book metadata into the database.
    :param session: Database session.
    :param limit: Maximum number of files to process.
    """
    folder = RDF_UNZIP_FOLDER_PATH / "cache/epub"
    count = 0

    for sub_folder in folder.iterdir():
        if not sub_folder.is_dir():
            continue

        for rdf_file in sub_folder.glob("*.rdf"):
            logger.info(f"Found RDF file: {rdf_file}")
            book = store_book_to_db(session, rdf_file)

            if book:
                logger.info(f"Book '{book.title}' stored successfully.")
            else:
                logger.warning(f"Failed to store book metadata from {rdf_file.name}.")

        count += 1
        if count >= limit:
            logger.info(f"Processing limit ({limit}) reached. Stopping.")
            break

def main() -> None:
    """
    Main entry point to prepare RDF data and process it.
    """
    prepare_rdf_data()

    with Session() as session:
        process_rdf_files(session)

if __name__ == "__main__":
    main()

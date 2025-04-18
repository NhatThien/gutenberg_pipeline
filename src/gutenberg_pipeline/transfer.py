import logging
from pathlib import Path
from typing import Optional

from gutenberg_pipeline import Book
from gutenberg_pipeline.database import Session
from gutenberg_pipeline.repositories import update_or_create_book
from gutenberg_pipeline.extract.content_cleaner import parse_book
from gutenberg_pipeline.extract.rdf_parser import parse_xml_file, extract_metadata

logger = logging.getLogger(__name__)

def fetch_metadata_from_rdf_file(rdf_file_path: Path) -> Optional[dict]:
    """
    Parse RDF file and extract metadata.
    """
    tree = parse_xml_file(rdf_file_path)
    if tree is None:
        raise ValueError(f"Failed to parse XML file: {rdf_file_path}")
    metadata = extract_metadata(tree)
    if metadata is None:
        raise ValueError(f"Failed to extract metadata from XML file: {rdf_file_path}")

    return metadata


def store_book_to_db(db: Session, file_path: Path) -> Optional[Book]:
    """
    Store book in the database.
    :param db: Database session.
    :param file_path:
    """
    metadata = fetch_metadata_from_rdf_file(file_path)
    if not metadata:
        logger.warning("No metadata found.")
        return None

    book_text = parse_book(metadata)
    book, created = update_or_create_book(db, book_id=metadata["gutenberg_id"], title=metadata["title"],
                              release_date=metadata.get("release_date"), book_link=metadata.get("book_link"),
                              language=metadata.get("language"), summary=metadata.get("summary"),
                              authors_data=metadata.get("authors"), categories_data=metadata.get("categories"),
                              content=book_text)
    logger.info(f"Book '{book.title}' {'created' if created else 'updated'} successfully.")

    return book

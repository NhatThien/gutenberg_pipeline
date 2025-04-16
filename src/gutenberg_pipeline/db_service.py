import logging
from typing import Type, Optional, Tuple
from sqlalchemy.exc import SQLAlchemyError
from gutenberg_pipeline.database import Session
from gutenberg_pipeline.models import Author, Book, BookTranslation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _handle_db_error(message: str) -> None:
    logger.exception(message)
    raise ValueError(message)


def create_author(db: Session, author_id: int, name: str,
                  birth_year: Optional[int], death_year: Optional[int]) -> Optional[Author]:
    try:
        author = Author(id=author_id, name=name, birth_year=birth_year, death_year=death_year)
        db.add(author)
        db.commit()
        db.refresh(author)
        logger.info(f"Author {name} created successfully.")
        return author
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error creating author: {e}")


def get_author(db: Session, name: str) -> Optional[Type[Author]]:
    try:
        return db.query(Author).filter(Author.name == name).first()
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error retrieving author: {e}")


def is_author_exists(db: Session, name: str) -> bool:
    return get_author(db, name) is not None


def delete_author(db: Session, author_id: int) -> None:
    try:
        author = db.query(Author).filter(Author.id == author_id).first()
        if author:
            db.delete(author)
            db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error deleting author: {e}")


# --- Book CRUD ---
def create_book(db: Session,
                title: str,
                language: Optional[str],
                content: Optional[str],
                authors: Optional[list[Author]],
                release_date: Optional[str], ) -> Optional[Tuple[Book, bool]]:
    try:
        existing = db.query(BookTranslation).filter(BookTranslation.title==title).first()
        if existing:
            return existing.book, False

        book = Book(release_date)
        book.authors = authors
        book_translation = BookTranslation(book= book, title=title, content=content, language=language)
        db.add(book)
        db.commit()
        db.refresh(book)

        return book, True
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error creating book: {e}")


def get_book_by_title(db: Session, title: str) -> Optional[Type[Book]]:
    try:
        book_translation = db.query(BookTranslation).filter(BookTranslation.title == title).first()
        return book_translation.book if book_translation else None
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error getting book by title: {e}")


def get_book_by_id(db: Session, book_id: int) -> Optional[Type[Book]]:
    try:
        book_translation = db.query(Book).filter(Book.id == book_id).first()
        return book_translation.book if book_translation else None
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error getting book by id: {e}")


def delete_book(db: Session, book_id: int) -> None:
    try:
        book = get_book_by_id(db, book_id=book_id)
        if book:
            db.delete(book)
            db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error deleting book by id: {e}")

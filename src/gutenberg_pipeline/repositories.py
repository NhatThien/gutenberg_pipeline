import logging
from typing import Optional, Tuple
from sqlalchemy.exc import SQLAlchemyError

from gutenberg_pipeline.database import Session
from gutenberg_pipeline.models import Author, Book, Category

logger = logging.getLogger(__name__)

def _handle_db_error(message: str) -> None:
    logger.error(message)
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


def get_author(db: Session, name: str) -> Optional[Author]:
    return db.query(Author).filter(Author.name == name).first()


def get_or_create_authors(db: Session, authors_data: list[dict]) -> Optional[list[Author]]:
    """
    Check if authors exist in the database, and create them if they don't.
    :param db: Database session.
    :param authors_data: List of authors data.
    :return: List of Author objects.
    """
    if not authors_data:
        return None

    author_objects = []
    for author_info in authors_data:
        author = get_author(db, author_info["name"])
        if not author:
            author = create_author(db, author_info["id"], author_info["name"],
                                   author_info.get("birth_year"), author_info.get("death_year"))
        if author:
            author_objects.append(author)

    return author_objects or None


def delete_author(db: Session, author_id: int) -> None:
    try:
        author = db.query(Author).filter(Author.id == author_id).first()
        if author:
            db.delete(author)
            db.commit()
        logger.info(f"Author {id} deleted successfully.")
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error deleting author: {e}")


def create_category(db: Session, category_name: str) -> Optional[Category]:
    try:
        category = Category(name=category_name)
        db.add(category)
        db.commit()
        db.refresh(category)
        logger.info(f"Category {category_name} created successfully.")
        return category
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error creating category: {e}")


def get_category(db: Session, name: str) -> Optional[Category]:
    return db.query(Category).filter(Category.name == name).first()


def get_or_create_categories(db: Session, categories_data: list[str]) -> Optional[list[Category]]:
    """
    Check if categories exist in the database, and create them if they don't.
    :param db: Database session.
    :param categories_data: List of categories data.
    :return: List of Author objects.
    """
    if not categories_data:
        return None

    category_objects = []
    for category_name in categories_data:
        category = get_category(db, category_name)
        if not category:
            category = create_category(db, category_name)
        if category:
            category_objects.append(category)

    return category_objects or None


def create_book(db: Session,
                book_id: int,
                title: str,
                authors: Optional[list[Author]],
                categories: Optional[list[Category]],
                release_date: Optional[str],
                gutenberg_link: Optional[str],
                summary: Optional[str],
                content: Optional[str],
                language: Optional[str],) -> Optional[Book]:
    try:
        book = Book(id = book_id, release_date=release_date, gutenberg_link=gutenberg_link,
                    title=title, summary=summary, content=content, language=language)
        if authors:
            book.authors = authors
        if categories:
            book.categories = categories
        db.add(book)
        db.commit()
        db.refresh(book)
        logger.info(f"Book {book_id} {title} created successfully.")
        return book
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error creating book: {e}")


def update_book(db: Session, book_id: int, **updates) -> Optional[Book]:
    """
    Update book information in the database.
    :param db: Database session.
    :param book_id: Book ID.
    :param updates: Dictionary of fields to update.
    :return: Updated Book object or NOne if book not found.
    """
    book = get_book_by_id(db, book_id)
    if not book:
        logger.warning(f"Book with ID {book_id} not found. Update skipped.")
        return None

    updated_fields = []
    for field_name, new_value in updates.items():
        if new_value is None or not hasattr(book, field_name):
            continue
        current_value = getattr(book, field_name)
        if current_value != new_value and isinstance(new_value, type(current_value)):
            setattr(book, field_name, new_value)
            updated_fields.append(field_name)
    if updated_fields:
        try:
            db.commit()
            db.refresh(book)
            logger.info(f"Book {book_id} updated successfully. Fields changed: {', '.join(updated_fields)}")
        except SQLAlchemyError as e:
            db.rollback()
            _handle_db_error(f"Error updating book: {e}")
    else:
        logger.info(f"No fields updated for Book {book_id}.")

    return book


def update_or_create_book(db: Session, book_id: int, title: str,
                authors_data: Optional[list[dict]],
                categories_data: Optional[list[str]],
                release_date: Optional[str],
                book_link: Optional[str],
                summary: Optional[str],
                content: Optional[str],
                language: Optional[str]) -> Optional[Tuple[Book, bool]]:
    """
    Check if the book exists in the database, and create it if it doesn't.
    :param content:
    :param language:
    :param summary:
    :param book_link:
    :param release_date:
    :param authors_data:
    :param categories_data:
    :param db: Database session.
    :param book_id: Book ID.
    :param title: Book title.
    :return: Book object.
    """
    try:
        authors = get_or_create_authors(db, authors_data) if authors_data else None
        categories = get_or_create_categories(db, categories_data) if categories_data else None

        book = get_book_by_id(db, book_id)
        if book:
            book = update_book(db, book_id=book_id, title=title,
                           release_date=release_date, gutenberg_link=book_link,
                           language=language, summary=summary,
                           authors=authors, categories=categories, content=content)

            return book, False
        else:
            book = create_book(db, book_id=book_id, title=title,
                               release_date=release_date, gutenberg_link=book_link,
                               language=language, summary=summary,
                               authors=authors, categories=categories, content=content)

            return book, True
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error updating or creating book: {e}")


def get_book_by_title(db: Session, title: str) -> Optional[Book]:
    return db.query(Book).filter(Book.title == title).first()


def get_book_by_id(db: Session, book_id: int) -> Optional[Book]:
    return db.query(Book).filter(Book.id == book_id).first()


def delete_book(db: Session, book_id: int) -> None:
    try:
        book = get_book_by_id(db, book_id=book_id)
        if book:
            db.delete(book)
            db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        _handle_db_error(f"Error deleting book by id: {e}")

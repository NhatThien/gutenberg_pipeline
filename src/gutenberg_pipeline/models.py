from sqlalchemy import Integer, String, Text, Date, ForeignKey, Table, Column
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gutenberg_pipeline.database import Base

book_author_table = Table(
    'book_author',
    Base.metadata,
    Column("book_id", Integer, ForeignKey('books.id'), primary_key=True),
    Column("author_id", Integer, ForeignKey('authors.id'), primary_key=True)
)

book_category_table = Table(
    'book_category',
    Base.metadata,
    Column("book_id", Integer, ForeignKey('books.id'), primary_key=True),
    Column("category_id", Integer, ForeignKey('categories.id'), primary_key=True)
)

class Author(Base):
    __tablename__ = 'authors'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    birth_year: Mapped[int] = mapped_column(Integer, nullable=True)
    death_year: Mapped[int] = mapped_column(Integer, nullable=True)

    books = relationship("Book", secondary=book_author_table, back_populates="authors")

class Book(Base):
    __tablename__ = 'books'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    release_date: Mapped[str] = mapped_column(Date)
    gutenberg_link = mapped_column(String(255), nullable=True)

    translations = relationship("BookTranslation", back_populates="book", cascade="all, delete-orphan")
    authors = relationship("Author", secondary="book_author", back_populates="books")


class BookTranslation(Base):
    __tablename__ = 'book_translations'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    translation_id: Mapped[int] = mapped_column(Integer, ForeignKey('books.id'))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    summary: Mapped[str] = mapped_column(Text)
    language: Mapped[str] = mapped_column(String(10))

    book = relationship("Book", back_populates="translations")


class Category(Base):
    __tablename__ = 'categories'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    books = relationship("Book", secondary=book_category_table, back_populates="categories")

# Gutenberg Pipeline

A full end-to-end pipeline to download, extract, clean, and store metadata and content from Project Gutenberg's catalog into a PostgreSQL database.

---

## 🔍 Features

- Download Project Gutenberg RDF catalog automatically
- Extract book metadata (title, authors, release date, categories, language, summary)
- Download and clean book text files
- Store structured data into PostgreSQL database
- Modern project structure (using `poetry`, `ruff`, `alembic`, `sqlalchemy`)
- Easily extendable for future improvements (search indexing, CLI tools, etc.)

---

## 📅 Quick Start

### 1. Install Dependencies

```bash
poetry install
```

### 2. Configure Database

Make sure PostgreSQL is running and create a database, for example:

```bash
createdb gutenberg_db
```

Edit `src/gutenberg_pipeline/config.py` and set your database URL if needed.

### 3. Run Migrations

```bash
alembic upgrade head
```

### 4. Run the Pipeline

```bash
poetry run python src/gutenberg_pipeline/main.py
```

This will:
- Download the RDF catalog if missing
- Extract RDF files
- Parse and insert book metadata and content into your database

---

## 🚀 Future Improvements

- CLI command support with `typer`
- Dockerfile for containerized deployment
- ElasticSearch indexing for full-text search
- Language auto-detection for missing languages
- Preprocessing / cleaning improvements (punctuation fix, stopword removal)
- Unit and integration testing

---

## 🔧 Tech Stack

- Python 3.12+
- Poetry
- SQLAlchemy
- Alembic
- PostgreSQL
- Requests
- BeautifulSoup
- Ruff (linting)

---

## ✨ Credits

Built by an open mind passionate about clean pipelines, solid project structures, and beautiful data. 💪

Project Gutenberg: [https://www.gutenberg.org/](https://www.gutenberg.org/)



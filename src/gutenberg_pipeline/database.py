from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from gutenberg_pipeline.config import Config

engine = create_engine(Config.DB_URI, echo=Config.ECHO_SQL)
Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

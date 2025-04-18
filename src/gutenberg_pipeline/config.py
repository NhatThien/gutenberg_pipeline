import os
from pathlib import Path
from load_dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY")
    DB_URI = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    DEBUG = os.getenv("ENV") == "development"
    ECHO_SQL = DEBUG
    DATA_FOLDER = Path(__file__).resolve().parent.parent.parent/"data"
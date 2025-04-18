import logging
import re
from typing import Optional

import requests

logger = logging.getLogger("__name__")

def parse_book(metadata: dict) -> Optional[str]:
    """
    Parse book metadata and return a dictionary.
    :param metadata: Metadata dictionary.
    :return: Parsed book dictionary.
    """
    if not metadata and not metadata.get("book_link"):
        return None
    try:
        request = requests.get(metadata["book_link"])
        request.raise_for_status()
        return extract_book_content(metadata.get("title"), request.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching book content: {e}")
        return None

def extract_book_content(title: str, text: str) -> str:
    """
    Retrieve only book content
    :param title:
    :param text:
    :return:
    """
    start_pattern = r"\*{3} start of the project GUTENBERG EBOOK "+title+r" \*{3}"
    end_pattern = r"\*{3} end of the project GUTENBERG EBOOK " + title + r" \*{3}"
    pattern = start_pattern+r"(.+)"+end_pattern
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        content = match.group(1).strip()
        if content:
            return content

    return text
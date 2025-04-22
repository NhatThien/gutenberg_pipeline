import logging
import re
from typing import Optional

from aiohttp import ClientSession, ClientError, http_exceptions
from asyncio import Semaphore

logger = logging.getLogger("__name__")

async def parse_book(semaphore: Semaphore, http_session: ClientSession, metadata: dict)\
        -> Optional[str]:
    """
    Parse book metadata and return a dictionary.
    :param semaphore:
    :param http_session:
    :param metadata: Metadata dictionary.
    :return: Parsed book dictionary.
    """
    if not metadata and not metadata.get("book_link"):
        return None
    try:
        async with semaphore:
            async with http_session.get(metadata["book_link"]) as response:
                try:
                    response.raise_for_status()
                    text = await response.text()
                    return extract_book_content(metadata.get("title"), text)
                except (ClientError, http_exceptions.HttpProcessingError) as e:
                    logger.error(f"Failed to fetch book {metadata['book_link']} content: {response.status} {e.message}")
                    return None
    except Exception as e:
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
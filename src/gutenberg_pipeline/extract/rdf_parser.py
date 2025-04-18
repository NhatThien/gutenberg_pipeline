import logging
from pathlib import Path
import xml.etree.ElementTree as ElementTree
from typing import Optional

NAMESPACES = {
    'pg': 'http://www.gutenberg.org/2009/pgterms/',
    'dc': 'http://purl.org/dc/terms/',
    'dcam': 'http://purl.org/dc/dcam/',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
}

logger = logging.getLogger(__name__)

def parse_xml_file(file_path: Path) -> Optional[ElementTree.ElementTree]:
    """
    Parse an XML file and return the ElementTree object.
    :param file_path: Path to the XML file.
    :return: ElementTree object.
    """
    try:
        tree = ElementTree.parse(file_path)
        return tree
    except ElementTree.ParseError as e:
        logger.error(f"Error parsing XML file {file_path}: {e}")
        return None

def extract_metadata(xml_tree: ElementTree.ElementTree) -> dict:
    """
    Extract metadata from the XML tree.
    :param xml_tree:
    :return:
    """
    root = xml_tree.getroot()

    def extract_id(text: str) -> Optional[str]:
        try:
            return text.split("/")[-1] if text else None
        except ValueError:
            logger.error("Error extracting ID: {e}")
            return None

    def get_text(tag: str) -> Optional[str]:
        el = root.find(tag, NAMESPACES)
        return el.text if el is not None else None

    def get_text_list(tag: str) -> Optional[list[str]]:
        el = root.findall(tag, NAMESPACES)
        return [e.text for e in el] if el else None

    def get_book_link() -> Optional[str]:
        for file_elem in root.findall(".//pg:file", NAMESPACES):
            format_value = file_elem.find(".//dc:format/rdf:Description/rdf:value", NAMESPACES)
            if format_value is not None and format_value.text.startswith("text/plain"):
                return file_elem.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")

        return None

    def get_book_id() -> Optional[int]:
        el = root.find('.//pg:ebook', NAMESPACES)
        if el is not None:
            return parse_int(extract_id(el.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")))

        return None

    def parse_int(text: Optional[str]) -> Optional[int]:
        try:
            return int(text) if text else None
        except (ValueError, TypeError):
            return None

    def get_nested_text(el: ElementTree.Element, tag: str) -> Optional[str]:
        child = el.find(tag, NAMESPACES)
        return child.text if child is not None else None

    def get_authors() -> Optional[list[dict]]:
        authors = root.findall('.//dc:creator', NAMESPACES)
        if not authors:
            return None
        res = []
        for author in authors:
            agent = author.find('.//pg:agent', NAMESPACES)
            author_id = extract_id(agent.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"))
            author_name = get_nested_text(author, './/pg:name')
            author_birth_year =  parse_int(get_nested_text(author, './/pg:birthdate'))
            author_death_year =  parse_int(get_nested_text(author,'.//pg:deathdate'))
            res.append(
                {
                    "id": author_id,
                    "name": author_name,
                    "birth_year": author_birth_year,
                    "death_year": author_death_year
                }
            )

        return res if res else None

    def get_bookshelves() -> Optional[list[str]]:
        bookshelves = get_text_list('.//pg:bookshelf/rdf:Description/rdf:value')
        if not bookshelves:
            return None
        res = []
        for bookshelf in bookshelves:
            if bookshelf.split(":")[0] != "Browsing":
                res.append(bookshelf)

        return res if res else None

    return {
        "gutenberg_id": get_book_id(),
        "categories": get_bookshelves(),
        "release_date": get_text('.//dc:issued'),
        "book_link": get_book_link(),
        "authors": get_authors(),
        "title": get_text('.//dc:title'),
        "summary": get_text('.//pg:marc520'),
        "language": get_text('.//dc:language/rdf:Description/rdf:value')
    }

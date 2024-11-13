import logging
import urllib.request
from urllib.request import urlopen


class URLAccessError(Exception):
    pass


class SPDXLicenseError(Exception):
    pass


def get_file_from_url(url: str, retries: int = 0) -> str:
    """Returns contents of data available at given URL"""
    if retries == 3:
        # Max number of retries
        logging.error(f"Unable to access {url}, URL invalid")
        raise URLAccessError(f"Unable to access {url}, URL invalid")
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_from_url(url, retries + 1)
    return body.decode("utf-8")

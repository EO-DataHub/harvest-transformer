from harvest_transformer.utils import get_file_from_url


def test_get_file_from_url_fail():
    url = "https://invalid-test-url.com"
    body = get_file_from_url(url)
    assert body is None


def test_get_file_from_url_pass():
    url = "https://test.eodatahub.org.uk/api/catalogue/stac/"
    body = get_file_from_url(url)
    assert body is not None

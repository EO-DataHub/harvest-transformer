import pytest

from harvest_transformer.utils import URLAccessError, get_file_from_url


def test_get_file_from_url_fail():
    url = "https://invalid-test-url.com"

    with pytest.raises(URLAccessError) as e_info:
        get_file_from_url(url)

    assert "URL invalid" in str(e_info.value)


def test_get_file_from_url_pass():
    url = "https://example.com/"
    body = get_file_from_url(url)
    assert body is not None

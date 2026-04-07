import pytest

from harvest_transformer import utils
from harvest_transformer.utils import URLAccessError, get_file_from_url


def test_load_json_url_parses_json(mocker):
    mocker.patch(
        "harvest_transformer.utils.get_file_from_url",
        return_value='{"sentinel2_ard":"sentinel-2_l1c_qa"}',
    )

    output = utils.load_json_url("https://collection-qa.s3.eu-west-2.amazonaws.com/qa-collection-map.json")

    assert output == {"sentinel2_ard": "sentinel-2_l1c_qa"}


def test_get_file_from_url_fail():
    url = "https://invalid-test-url.com"

    with pytest.raises(URLAccessError) as e_info:
        get_file_from_url(url)

    assert "URL invalid" in str(e_info.value)


def test_get_file_from_url_pass():
    url = "https://example.com/"
    body = get_file_from_url(url)
    assert body is not None

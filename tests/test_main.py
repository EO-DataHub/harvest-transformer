import json
import os
from unittest import mock

import botocore

from harvest_transformer.transformer import (
    apply_patch,
    get_new_catalog_id_from_target,
    get_patch,
    is_valid_url,
    reformat_key,
    transform_key,
    update_catalog_id,
)


def test_reformat_key_no_git_harvester():
    file_name = "https://test-catalog.temp.data.com/collections/test-collection"
    source = "https://test-catalog.temp.data.com"
    target = "test-datasets/test-catalog"
    transformed_key = transform_key(file_name, source, target)

    assert transformed_key == "test-datasets/test-catalog/test-collection.json"


def test_reformat_key_git_harvester():
    file_name = "git-harvester/test-datasets/test-catalog/collections/test-collection"
    source = "/"
    target = "/"
    transformed_key = transform_key(file_name, source, target)

    assert transformed_key == "test-datasets/test-catalog/test-collection.json"


def test_reformat_key_catalog():
    key = "transformed/test-datasets/test-catalog"
    reformatted_key = reformat_key(key)
    assert reformatted_key == "transformed/test-datasets/test-catalog.json"


def test_reformat_key_collection():
    key = "transformed/test-datasets/test-catalog/collections/test-collection"
    reformatted_key = reformat_key(key)
    assert reformatted_key == "transformed/test-datasets/test-catalog/test-collection.json"


@mock.patch("harvest_transformer.transformer.s3_client.get_object")
@mock.patch.dict(os.environ, {"PATCH_PREFIX": "patches"})
def test_get_patch_found(mock_get_object):
    # Mock the S3 response
    mock_response = {
        "Body": mock.Mock(
            read=mock.Mock(return_value=json.dumps({"patch": "data"}).encode("utf-8"))
        )
    }
    mock_get_object.return_value = mock_response

    file_name = "supported-datasets/ceda-stac-catalogue/cmip6"
    patch_bucket = "catalogue-population-eodhp-dev"

    patch_data = get_patch(file_name, patch_bucket)

    assert patch_data is not None
    assert patch_data == {"patch": "data"}
    mock_get_object.assert_called_once_with(
        Bucket=patch_bucket, Key="patches/supported-datasets/ceda-stac-catalogue/cmip6"
    )


@mock.patch("harvest_transformer.transformer.s3_client.get_object")
@mock.patch.dict(os.environ, {"PATCH_PREFIX": "patches"})
def test_get_patch_not_found(mock_get_object):
    # Mock the S3 ClientError for NoSuchKey
    mock_get_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "get_object"
    )

    file_name = "supported-datasets/ceda-stac-catalogue/cmip6"
    patch_bucket = "catalogue-population-eodhp-dev"

    patch_data = get_patch(file_name, patch_bucket)

    assert patch_data is None
    mock_get_object.assert_called_once_with(
        Bucket=patch_bucket, Key="patches/supported-datasets/ceda-stac-catalogue/cmip6"
    )


@mock.patch("harvest_transformer.transformer.s3_client.get_object")
@mock.patch.dict(os.environ, {"PATCH_PREFIX": "patches"})
def test_get_patch_unexpected_error(mock_get_object):
    # Mock an unexpected error
    mock_get_object.side_effect = Exception("Unexpected error")

    file_name = "supported-datasets/ceda-stac-catalogue/cmip6"
    patch_bucket = "catalogue-population-eodhp-dev"

    patch_data = get_patch(file_name, patch_bucket)

    assert patch_data is None
    mock_get_object.assert_called_once_with(
        Bucket=patch_bucket, Key="patches/supported-datasets/ceda-stac-catalogue/cmip6"
    )


def test_apply_patch_success():
    original = {"key": "value"}
    patch = [{"op": "replace", "path": "/key", "value": "new_value"}]

    patched = apply_patch(original, patch)

    assert patched == {"key": "new_value"}


def test_apply_patch_failure():
    original = {"key": "value"}
    patch = [{"op": "replace", "path": "/nonexistent", "value": "new_value"}]

    patched = apply_patch(original, patch)

    assert patched == original


def test_reformat_key_item():
    key = "transformed/test-datasets/test-catalog/collections/test-collection/items/test-item"
    reformatted_key = reformat_key(key)
    assert (
        reformatted_key == "transformed/test-datasets/test-catalog/test-collection/test-item.json"
    )


def test_get_new_catalog_id_from_target():
    target = "test-datasets/test-catalog"
    catalog_id = get_new_catalog_id_from_target(target)
    assert catalog_id == "test-catalog"


def test_update_catalog_id_catalog():
    entry_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [
            {"rel": "root", "href": "https://test-key/catalog/"},
            {"rel": "self", "href": "https://test-key/catalog/"},
        ],
    }
    new_id = "updated_test_id"
    target = f"test-catalog/{new_id}"

    new_entry_body = update_catalog_id(entry_body, target)

    assert new_entry_body["type"] == entry_body["type"]
    assert new_entry_body["title"] == entry_body["title"]
    assert new_entry_body["id"] == new_id


def test_update_catalog_id_collection():
    entry_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [{"rel": "root", "href": "https://test-key/catalog/"}],
    }
    target = "test-catalog/new_test_id"

    new_entry_body = update_catalog_id(entry_body, target)

    assert new_entry_body == entry_body


def test_update_catalog_id_item():
    entry_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [{"rel": "root", "href": "https://test-key/catalog/"}],
    }
    target = "test-catalog/new_test_id"

    new_entry_body = update_catalog_id(entry_body, target)

    assert new_entry_body == entry_body


def test_is_valid_url_pass():
    valid_url = "https://example.com"
    assert is_valid_url(valid_url)


def test_is_valid_url_fail():
    invalid_url = "%https://invalid-test-url.com"
    assert not is_valid_url(invalid_url)


class Message:
    def __init__(self, msg):
        self.msg = json.dumps(msg).encode("utf-8")

    def data(self):
        return self.msg

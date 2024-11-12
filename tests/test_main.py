import json
import unittest.mock
from unittest.mock import patch

from botocore.exceptions import ClientError

from harvest_transformer.__main__ import (
    delete_file_s3,
    get_file_contents_as_json,
    get_file_s3,
    get_new_catalog_id_from_target,
    is_valid_url,
    process_pulsar_message,
    reformat_key,
    transform_key,
    update_catalog_id,
    upload_file_s3,
)


def test_reformat_key_no_git_harvester():
    file_name = "https://test-catalog.temp.data.com/collections/test-collection"
    source = "https://test-catalog.temp.data.com"
    target = "test-datasets/test-catalog"
    transformed_key = transform_key(file_name, source, target)

    assert transformed_key == "transformed/test-datasets/test-catalog/test-collection.json"


def test_reformat_key_git_harvester():
    file_name = "git-harvester/test-datasets/test-catalog/collections/test-collection"
    source = "/"
    target = "/"
    transformed_key = transform_key(file_name, source, target)

    assert transformed_key == "transformed/test-datasets/test-catalog/test-collection.json"


def test_reformat_key_catalog():
    key = "transformed/test-datasets/test-catalog"
    reformatted_key = reformat_key(key)
    assert reformatted_key == "transformed/test-datasets/test-catalog.json"


def test_reformat_key_collection():
    key = "transformed/test-datasets/test-catalog/collections/test-collection"
    reformatted_key = reformat_key(key)
    assert reformatted_key == "transformed/test-datasets/test-catalog/test-collection.json"


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
    file_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [{"rel": "root", "href": "https://test-key/catalog/"}],
    }
    new_id = "updated_test_id"
    target = f"test-catalog/{new_id}"
    key = "https://test-key/catalog/"

    new_file_body = update_catalog_id(file_body, target, key)

    assert new_file_body["type"] == file_body["type"]
    assert new_file_body["title"] == file_body["title"]
    assert new_file_body["id"] == new_id


def test_update_catalog_id_collection():
    file_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [{"rel": "root", "href": "https://test-key/catalog/"}],
    }
    target = "test-catalog/new_test_id"
    key = "https://test-key/catalog/collection"

    new_file_body = update_catalog_id(file_body, target, key)

    assert new_file_body == file_body


def test_update_catalog_id_item():
    file_body = {
        "id": "test_id",
        "type": "Catalog",
        "title": "test_title",
        "links": [{"rel": "root", "href": "https://test-key/catalog/"}],
    }
    target = "test-catalog/new_test_id"
    key = "https://test-key/catalog/collection/item"

    new_file_body = update_catalog_id(file_body, target, key)

    assert new_file_body == file_body


def test_is_valid_url_pass():
    valid_url = "https://example.com"
    assert is_valid_url(valid_url)


def test_is_valid_url_fail():
    invalid_url = "%https://invalid-test-url.com"
    assert not is_valid_url(invalid_url)


def test_upload_file_s3():

    # Test data
    body = "test data"
    bucket = "test-bucket"
    key = "test/key.txt"

    # Mock the S3 client
    with unittest.mock.patch("harvest_transformer.__main__.s3_client") as mock_s3_client:
        # Call upload_file_s3
        upload_file_s3(body, bucket, key)

        # Assert put_object was called correctly
        mock_s3_client.put_object.assert_called_once_with(Body=body, Bucket=bucket, Key=key)


def test_delete_file_s3():

    # Test data
    bucket = "test-bucket"
    key = "test/key.txt"

    # Mock the S3 client
    with unittest.mock.patch("harvest_transformer.__main__.s3_client") as mock_s3_client:
        # Call upload_file_s3
        delete_file_s3(bucket, key)

        # Assert delete_object was called correctly
        mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket, Key=key)


def test_get_file_s3():

    # Test data
    bucket = "test-bucket"
    key = "test/key.txt"

    # Mock the S3 client
    with unittest.mock.patch("harvest_transformer.__main__.s3_client") as mock_s3_client:
        # Call upload_file_s3
        get_file_s3(bucket, key)

        # Assert delete_object was called correctly
        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)


@patch("harvest_transformer.__main__.get_file_from_url")
def test_get_file_contents_as_json_url(mock_get_file_from_url):
    # Mock return value for get_file_from_url
    mock_get_file_from_url.return_value = '{"key": "value from URL"}'

    # Call the function with a URL
    result = get_file_contents_as_json("https://test-catalog.temp.data.com")

    # Assert the expected result
    assert result == {"key": "value from URL"}

    # Ensure the mock was called
    mock_get_file_from_url.assert_called_once_with("https://test-catalog.temp.data.com")


@patch("harvest_transformer.__main__.get_file_s3")
def test_get_file_contents_as_json_s3(mock_get_file_s3):
    # Mock return value for get_file_from_url
    mock_get_file_s3.return_value = '{"key": "value from S3"}'

    # Call the function with a URL
    result = get_file_contents_as_json("file.json", "dummy_bucket")

    # Assert the expected result
    assert result == {"key": "value from S3"}

    # Ensure the mock was called
    mock_get_file_s3.assert_called_once_with("dummy_bucket", "file.json")


class Message:
    def __init__(self, msg):
        self.msg = json.dumps(msg).encode("utf-8")

    def data(self):
        return self.msg


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.update_file")
@patch("harvest_transformer.__main__.upload_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_add(
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_update_file,
    mock_map_licence_codes_to_filenames,
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []
    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_upload_file_s3.return_value = True

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "deleted_keys": [],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that update_file was called once for each file
    assert mock_update_file.call_count == 3

    # Assert that the upload to S3 function was called once per file
    assert mock_upload_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
        "transformed/test-datasets/test-catalog/test-collection.json",
        "transformed/test-datasets/test-catalog/test-collection/test-item.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformated
    assert output_data["added_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert no failed files
    assert output_data["failed_files"]["temp_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["deleted_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["deleted_keys"] == []


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.update_file")
@patch("harvest_transformer.__main__.upload_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_update(
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_update_file,
    mock_map_licence_codes_to_filenames,
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []
    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_upload_file_s3.return_value = True

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "added_keys": [],
        "deleted_keys": [],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that update_file was called once for each file
    assert mock_update_file.call_count == 3

    # Assert that the upload to S3 function was called once per file
    assert mock_upload_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
        "transformed/test-datasets/test-catalog/test-collection.json",
        "transformed/test-datasets/test-catalog/test-collection/test-item.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformated
    assert output_data["updated_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert no failed files
    assert output_data["failed_files"]["temp_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["deleted_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["deleted_keys"] == []


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.update_file")
@patch("harvest_transformer.__main__.delete_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_delete(
    mock_get_file_from_url,
    mock_delete_file_s3,
    mock_update_file,
    mock_map_licence_codes_to_filenames,
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []
    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_delete_file_s3.return_value = True

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": [],
        "deleted_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that update_file was not called
    assert mock_update_file.call_count == 0

    # Assert that the delete from S3 function was called once per file
    assert mock_delete_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
        "transformed/test-datasets/test-catalog/test-collection.json",
        "transformed/test-datasets/test-catalog/test-collection/test-item.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformated
    assert output_data["deleted_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert no failed files
    assert output_data["failed_files"]["temp_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["deleted_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["deleted_keys"] == []


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.update_file")
@patch("harvest_transformer.__main__.upload_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_failure(
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_update_file,
    mock_map_licence_codes_to_filenames,
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []
    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_upload_file_s3.side_effect = [
        None,  # First call succeeds
        ClientError(
            {"Error": {"Code": "500"}}, "upload_file_s3"
        ),  # Second call fails with temporary error
        Exception("Permanent error"),  # Third call fails with permanent error
    ]

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "deleted_keys": [],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that update_file was called once for each file
    assert mock_update_file.call_count == 3

    # Assert that the upload to S3 function was called once per file
    assert mock_upload_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformatted
    assert output_data["added_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert failed files
    assert len(output_data["failed_files"]["temp_failed_keys"]["added_keys"]) == 1
    assert (
        output_data["failed_files"]["temp_failed_keys"]["added_keys"][0]
        == "https://test-catalog.temp.data.com/collections/test-collection"
    )
    assert len(output_data["failed_files"]["perm_failed_keys"]["added_keys"]) == 1
    assert (
        output_data["failed_files"]["perm_failed_keys"]["added_keys"][0]
        == "https://test-catalog.temp.data.com/collections/test-collection/items/test-item"
    )


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
def test_process_pulsar_message_failure_invalid_url(mock_map_licence_codes_to_filenames):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "deleted_keys": [],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformatted
    assert len(output_data["added_keys"]) == len(output_data["added_keys"]) == 0
    assert list(output_data.keys()) == list(error_data.keys()) == expected_dict_keys
    # Assert failed files
    assert len(error_data["failed_files"]["temp_failed_keys"]["added_keys"]) == 0
    assert len(error_data["failed_files"]["perm_failed_keys"]["added_keys"]) == 3
    assert (
        error_data["failed_files"]["perm_failed_keys"]["added_keys"][0]
        == "https://test-catalog.temp.data.com"
    )


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.delete_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_deleted_keys(
    mock_get_file_from_url, mock_delete_file_s3, mock_map_licence_codes_to_filenames
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []
    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_delete_file_s3.return_value = True

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [],
        "added_keys": [],
        "deleted_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that delete_file_s3 was called once for each file
    assert mock_delete_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
        "transformed/test-datasets/test-catalog/test-collection.json",
        "transformed/test-datasets/test-catalog/test-collection/test-item.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformatted
    assert output_data["deleted_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert no failed files
    assert output_data["failed_files"]["temp_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["deleted_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["deleted_keys"] == []


@patch("harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames")
@patch("harvest_transformer.__main__.update_file")
@patch("harvest_transformer.__main__.upload_file_s3")
@patch("harvest_transformer.__main__.get_file_from_url")
def test_process_pulsar_message_updated_keys(
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_update_file,
    mock_map_licence_codes_to_filenames,
):
    # Mock map_licence_codes_to_filenames to return an empty list
    mock_map_licence_codes_to_filenames.return_value = []

    # Mock return value for get_file_from_url
    stac_location = "test_data/test_links_replacement_only.json"
    with open(stac_location, "r") as file:
        mock_get_file_from_url.return_value = file.read()

    # Mock this to prevent any actual S3 integration
    mock_upload_file_s3.return_value = True

    # Input Pulsar message
    msg = {
        "id": "pulsar-test-id/testing",
        "workspace": "",
        "repository": "",
        "branch": "main",
        "bucket_name": "test-bucket",
        "updated_keys": [
            "https://test-catalog.temp.data.com",
            "https://test-catalog.temp.data.com/collections/test-collection",
            "https://test-catalog.temp.data.com/collections/test-collection/items/test-item",
        ],
        "added_keys": [],
        "deleted_keys": [],
        "source": "https://test-catalog.temp.data.com",
        "target": "test-datasets/test-catalog",
    }

    test_message = Message(msg)
    output_root = "/"
    output_data, error_data = process_pulsar_message(test_message, output_root)  # Unpack the tuple

    # Assert that update_file was called once for each file
    assert mock_update_file.call_count == 3

    # Assert that the upload to S3 function was called once per file
    assert mock_upload_file_s3.call_count == 3

    expected_output_keys = [
        "transformed/test-datasets/test-catalog.json",
        "transformed/test-datasets/test-catalog/test-collection.json",
        "transformed/test-datasets/test-catalog/test-collection/test-item.json",
    ]

    expected_dict_keys = [
        "id",
        "workspace",
        "repository",
        "branch",
        "bucket_name",
        "updated_keys",
        "added_keys",
        "deleted_keys",
        "source",
        "target",
        "failed_files",
    ]

    # Assert output keys are correctly reformatted
    assert output_data["updated_keys"] == expected_output_keys
    assert list(output_data.keys()) == expected_dict_keys

    # Assert no failed files
    assert output_data["failed_files"]["temp_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["updated_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["added_keys"] == []
    assert output_data["failed_files"]["temp_failed_keys"]["deleted_keys"] == []
    assert output_data["failed_files"]["perm_failed_keys"]["deleted_keys"] == []

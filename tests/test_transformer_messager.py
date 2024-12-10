import json
from unittest import mock

import boto3
import moto
from eodhp_utils.messagers import Messager

from harvest_transformer.transformer_messager import TransformerMessager


@mock.patch("harvest_transformer.transformer_messager.TransformerMessager.process_update_body")
def test_process_update(mock_process_update_body):

    with moto.mock_aws():
        stac_item = {"id": "test-item"}
        # Create S3 resources and upload file to S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        s3.put_object(
            Body=json.dumps(stac_item),
            Bucket="test-bucket",
            Key="stac-harvester/test/key/path.json",
        )

        mock_producer = mock.MagicMock()
        mock_process_update_body.return_value = [
            Messager.OutputFileAction(
                file_body=stac_item,
                cat_path="test-catalog/key/path.json",
            )
        ]

        test_transformer_messager = TransformerMessager(
            s3_client=s3,
            output_bucket="files_bucket_name",
            cat_output_prefix="transformed",
            producer=mock_producer,
        )

        result = test_transformer_messager.process_update(
            "test-bucket",
            "stac-harvester/test/key/path.json",
            "test/key/path.json",
            "test/",
            "test-catalog/",
        )

        mock_process_update_body.assert_called_once_with(
            stac_item, "test/key/path.json", "test/", "test-catalog/"
        )

        assert result == [
            Messager.OutputFileAction(
                file_body=stac_item,
                cat_path="test-catalog/key/path.json",
            )
        ]


@mock.patch("harvest_transformer.transformer_messager.TransformerMessager.get_workspace_from_msg")
@mock.patch("harvest_transformer.transformer_messager.transform")
@mock.patch.dict("os.environ", {"OUTPUT_ROOT": "https://test-url-root.org.uk"})
def test_process_update_body(mock_transform, mock_get_workspace_from_msg):
    stac_item = {"id": "test-item"}
    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_transform.return_value = stac_item
    mock_get_workspace_from_msg.return_value = "test-workspace"
    test_transformer_messager = TransformerMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="transformed",
        producer=mock_producer,
    )

    result = test_transformer_messager.process_update_body(
        stac_item, "test/key/path.json", "test/", "test-catalog/"
    )

    expected_action = Messager.OutputFileAction(
        file_body=stac_item,
        cat_path="test-catalog/key/path.json",
    )

    mock_transform.assert_called_once_with(
        entry_body=stac_item,
        source="test/",
        target="test-catalog/",
        output_root="https://test-url-root.org.uk",
        workspace="test-workspace",
    )

    assert result == [expected_action]


@mock.patch("harvest_transformer.transformer_messager.TransformerMessager.get_workspace_from_msg")
@mock.patch("harvest_transformer.transformer_messager.transform")
@mock.patch.dict("os.environ", {"OUTPUT_ROOT": "https://test-url-root.org.uk"})
def test_process_delete(mock_transform, mock_get_workspace_from_msg):
    stac_item = {"id": "test-item"}
    mock_s3_client = mock.MagicMock()
    mock_producer = mock.MagicMock()
    mock_transform.return_value = stac_item
    mock_get_workspace_from_msg.get.return_value = "test-workspace"
    test_transformer_messager = TransformerMessager(
        s3_client=mock_s3_client,
        output_bucket="files_bucket_name",
        cat_output_prefix="transformed",
        producer=mock_producer,
    )

    result = test_transformer_messager.process_delete(
        "test-bucket",
        "stac-harvester/test/key/path.json",
        "test/key/path.json",
        "test/",
        "test-catalog/",
    )

    expected_action = Messager.OutputFileAction(
        file_body=None,
        cat_path="test-catalog/key/path.json",
    )

    assert result == [expected_action]

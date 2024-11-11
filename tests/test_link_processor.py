import copy
import json
import os
from unittest.mock import Mock, patch

import pytest

from harvest_transformer.__main__ import update_file
from harvest_transformer.link_processor import LinkProcessor
from harvest_transformer.workflow_processor import WorkflowProcessor

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"

# Ensure you update this list when other transformers are added
with patch(
    "harvest_transformer.link_processor.LinkProcessor.list_s3_license_files"
) as mock_list_s3_license_files:
    mock_list_s3_license_files.return_value = []
    PROCESSORS = [WorkflowProcessor(), LinkProcessor()]


# Define the fixture
@pytest.fixture
def link_processor_fixture(mocker):
    with patch(
        "harvest_transformer.link_processor.LinkProcessor.list_s3_license_files", new_callable=Mock
    ) as mock_list_s3_license_files:
        mock_list_s3_license_files.return_value = ["AAL"]
        mocker.patch.dict(
            os.environ, {"HOSTED_ZONE": "test-url.org.uk", "S3_BUCKET": "SPDX_BUCKET"}
        )
        processor = LinkProcessor()
        yield processor


def test_links_replacement_only(link_processor_fixture):
    # Configure mocks
    link_processor = [LinkProcessor()]
    stac_location = "test_data/test_links_replacement_only.json"
    # Load test STAC data
    with open(stac_location, "r") as file:
        json_data = json.load(file)
        input_data = copy.deepcopy(json_data)

    # Execute update file process
    output = update_file(
        file_name=stac_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        file_body=json_data,
        output_root=OUTPUT_ROOT,
        processors=link_processor,
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Construct expected output links
    for link in input_data["links"]:
        if link["rel"] == "self":
            expect_self_link = link
            expect_self_link.update(
                {
                    "href": "https://output.root.test/target_directory/collections/example_collection/items/example_stac_feature"
                }
            )
        elif link["rel"] == "root":
            expect_root_link = link
            expect_root_link.update({"href": "https://output.root.test/target_directory/"})
        elif link["rel"] == "parent":
            expect_parent_link = link
            expect_parent_link.update(
                {"href": "https://output.root.test/target_directory/collections/example_collection"}
            )
        elif link["rel"] == "collection":
            expect_collection_link = link
            expect_collection_link.update(
                {"href": "https://output.root.test/target_directory/collections/example_collection"}
            )

    expected_links = [
        expect_self_link,
        expect_root_link,
        expect_parent_link,
        expect_collection_link,
    ]

    # Check other data is unchanged
    for key in output_json:
        if not key == "links":
            assert output_json[key] == input_data[key]

    assert output_json["links"] == expected_links


def test_links_add_missing_links(link_processor_fixture):
    # Configure mocks
    link_processor = [LinkProcessor()]
    stac_location = "test_data/test_links_add_missing_links.json"
    # Load test STAC data
    with open(stac_location, "r") as file:
        json_data = json.load(file)
        input_data = copy.deepcopy(json_data)

    # Execute update file process
    output = update_file(
        file_name=stac_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        file_body=json_data,
        output_root=OUTPUT_ROOT,
        processors=link_processor,
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    expected_links = [
        {"rel": "self", "href": f"{OUTPUT_ROOT}{TARGET}{stac_location}"},
        {"rel": "root", "href": OUTPUT_ROOT},
    ]

    # Check other data is unchanged
    for key in output_json:
        if not key == "links":
            assert output_json[key] == input_data[key]

    assert output_json["links"] == expected_links


def test_workflow_does_not_alter_non_workflows():
    stac_location = "test_data/test_links_replacement_only.json"
    # Load test STAC data
    with open(stac_location, "r") as file:
        json_data = json.load(file)
        input_data = copy.deepcopy(json_data)

    # Execute update file process
    output = update_file(
        file_name=stac_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        file_body=json_data,
        output_root=OUTPUT_ROOT,
        processors=PROCESSORS,
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Construct expected output links
    for link in input_data["links"]:
        if link["rel"] == "self":
            expect_self_link = link
            expect_self_link.update(
                {
                    "href": "https://output.root.test/target_directory/collections/example_collection/items/example_stac_feature"
                }
            )
        elif link["rel"] == "root":
            expect_root_link = link
            expect_root_link.update({"href": "https://output.root.test/target_directory/"})
        elif link["rel"] == "parent":
            expect_parent_link = link
            expect_parent_link.update(
                {"href": "https://output.root.test/target_directory/collections/example_collection"}
            )
        elif link["rel"] == "collection":
            expect_collection_link = link
            expect_collection_link.update(
                {"href": "https://output.root.test/target_directory/collections/example_collection"}
            )

    expected_links = [
        expect_self_link,
        expect_root_link,
        expect_parent_link,
        expect_collection_link,
    ]

    # Check other data is unchanged
    for key in output_json:
        if not key == "links":
            assert output_json[key] == input_data[key]

    assert output_json["links"] == expected_links


def test_add_new_license_link(link_processor_fixture):
    json_data = {"links": []}
    processor = LinkProcessor()
    processor.spdx_license_list = ["APL-1.0"]
    processor.add_license_link(
        json_data,
        "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
    )
    assert json_data["links"] == [
        {
            "rel": "license",
            "type": "text/plain",
            "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        }
    ]


def test_add_new_license_link_from_id(link_processor_fixture):
    processor = LinkProcessor()
    processor.spdx_license_list = ["AAL"]
    json_data = {"links": [], "license": "aal"}
    processor.ensure_license_links(json_data)
    assert json_data["links"] == [
        {
            "rel": "license",
            "type": "text/plain",
            "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/text/AAL.txt",
        },
        {
            "rel": "license",
            "type": "text/html",
            "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/html/AAL.html",
        },
    ]


def test_dont_add_license_link_when_present(link_processor_fixture):
    processor = LinkProcessor()
    processor.spdx_license_list = ["AAL"]
    json_data = {
        "links": [
            {
                "rel": "license",
                "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
            }
        ]
    }
    processor.ensure_license_links(json_data)
    assert json_data == {
        "links": [
            {
                "rel": "license",
                "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
            }
        ]
    }


def test_add_multiple_license_links(link_processor_fixture):
    processor = LinkProcessor()
    processor.spdx_license_list = ["AAL", "APSL-1.2"]
    json_data = {
        "links": [
            {
                "rel": "license",
                "type": "text/plain",
                "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
            }
        ]
    }
    processor.add_license_link(
        json_data,
        "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/html/APSL-1.2.html",
    )
    assert json_data["links"] == [
        {
            "rel": "license",
            "type": "text/plain",
            "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        },
        {
            "rel": "license",
            "type": "text/html",
            "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/html/APSL-1.2.html",
        },
    ]


def test_add_license_link_to_existing_links(link_processor_fixture):
    processor = LinkProcessor()
    processor.spdx_license_list = ["AAL"]
    json_data = {
        "license": "AAL",
        "links": [
            {"rel": "self", "href": "https://example.com/self"},
            {"rel": "parent", "href": "https://example.com/parent"},
        ],
    }
    processor.ensure_license_links(json_data)
    assert json_data["links"] == [
        {"rel": "self", "href": "https://example.com/self"},
        {"rel": "parent", "href": "https://example.com/parent"},
        {
            "rel": "license",
            "type": "text/plain",
            "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/text/AAL.txt",
        },
        {
            "rel": "license",
            "type": "text/html",
            "href": "https://test-url.org.uk/harvested/default/spdx/license-list-data/main/html/AAL.html",
        },
    ]


def test_add_license_link_unknown_license_id(link_processor_fixture):
    processor = LinkProcessor()
    json_data = {
        "license": "proprietary",
        "links": [
            {"rel": "self", "href": "https://example.com/self"},
            {"rel": "parent", "href": "https://example.com/parent"},
        ],
    }
    processor.ensure_license_links(
        json_data,
    )
    assert json_data["links"] == [
        {"rel": "self", "href": "https://example.com/self"},
        {"rel": "parent", "href": "https://example.com/parent"},
    ]

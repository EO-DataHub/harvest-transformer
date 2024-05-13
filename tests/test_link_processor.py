import copy
import json

from harvest_transformer.__main__ import update_file
from harvest_transformer.link_processor import LinkProcessor
from harvest_transformer.workflow_processor import WorkflowProcessor

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"

# Ensure you update this list when other transformers are added
PROCESSORS = [WorkflowProcessor(), LinkProcessor()]


def test_links_replacement_only():
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
        file_json=json_data,
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


def test_links_add_missing_links():
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
        file_json=json_data,
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
        file_json=json_data,
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

import json
from uuid import UUID

from harvest_transformer.file_processor import FileProcessor

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


def is_valid_uuid(uuid_to_test, version=4):

    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


def test_workflows_minimal_input():
    stac_location = "test_data/workflow-test-stac-1.json"
    stac_expected_location = "test_data/workflow-test-stac-1-expected.json"
    file_processor = FileProcessor()

    # Load test STAC data
    with open(stac_location, "r") as file:
        json_data = json.load(file)

    # Execute update file process
    output = file_processor.update_file(stac_location, SOURCE_PATH, TARGET, json_data, OUTPUT_ROOT)

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location, "r") as file:
        expected_json = json.load(file)

    # Check generated stac is complete and sound
    output_keys = list(output_json.keys())
    expected_keys = list(expected_json.keys())
    assert output_keys == expected_keys

    # Check generated STAC is correct
    for key in output_json:
        assert output_json[key] == expected_json[key]

    # Expected output links
    for link in expected_json["links"]:
        if link["rel"] == "self":
            expect_self_link = link
        elif link["rel"] == "root":
            expect_root_link = link

    # Check updates links are correct
    for link in output_json["links"]:
        if link["rel"] == "self":
            self_link = link
        elif link["rel"] == "root":
            root_link = link

    assert self_link["href"] == expect_self_link["href"]
    assert root_link["href"] == expect_root_link["href"]


def test_workflows_summaries():
    stac_input_location = "test_data/workflow-test-stac-2.json"
    stac_expected_location = "test_data/workflow-test-stac-2-expected.json"
    file_processor = FileProcessor()

    # Load test STAC data
    with open(stac_input_location, "r") as file:
        json_data = json.load(file)

    # Execute update file process
    output = file_processor.update_file(
        stac_input_location, SOURCE_PATH, TARGET, json_data, OUTPUT_ROOT
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location, "r") as file:
        expected_json = json.load(file)

    # Check generated stac is complete and sound
    output_keys = list(output_json.keys())
    expected_keys = list(expected_json.keys())
    assert output_keys == expected_keys

    # Check generated STAC is correct
    for key in output_json:
        assert output_json[key] == expected_json[key]

    # Expected output links
    for link in expected_json["links"]:
        if link["rel"] == "self":
            expect_self_link = link
        elif link["rel"] == "root":
            expect_root_link = link

    # Check updates links are correct
    for link in output_json["links"]:
        if link["rel"] == "self":
            self_link = link
        elif link["rel"] == "root":
            root_link = link

    assert self_link["href"] == expect_self_link["href"]
    assert root_link["href"] == expect_root_link["href"]


def test_workflows_no_id():
    stac_input_location = "test_data/workflow-test-stac-3.json"
    stac_expected_location = "test_data/workflow-test-stac-3-expected.json"
    file_processor = FileProcessor()

    # Load test STAC data
    with open(stac_input_location, "r") as file:
        json_data = json.load(file)

    # Execute update file process
    output = file_processor.update_file(
        stac_input_location, SOURCE_PATH, TARGET, json_data, OUTPUT_ROOT
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location, "r") as file:
        expected_json = json.load(file)

    # Check generated stac is complete and sound
    output_keys = list(output_json.keys())
    expected_keys = list(expected_json.keys())
    assert output_keys == expected_keys

    # Check generated STAC is correct
    for key in output_json:
        if key in ("id", "title"):
            id_uuid = output_json[key].rsplit("__")[1]
            assert is_valid_uuid(id_uuid)
        else:
            assert output_json[key] == expected_json[key]

    # Expected output links
    for link in expected_json["links"]:
        if link["rel"] == "self":
            expect_self_link = link
        elif link["rel"] == "root":
            expect_root_link = link

    # Check updates links are correct
    for link in output_json["links"]:
        if link["rel"] == "self":
            self_link = link
        elif link["rel"] == "root":
            root_link = link

    assert self_link["href"] == expect_self_link["href"]
    assert root_link["href"] == expect_root_link["href"]

import copy
import json
from unittest.mock import patch
from uuid import UUID

from harvest_transformer.link_processor import LinkProcessor
from harvest_transformer.transformer import update_file
from harvest_transformer.workflow_processor import WorkflowProcessor

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"

# Ensure you update this list when other transformers are added
with patch(
    "harvest_transformer.link_processor.LinkProcessor.map_licence_codes_to_filenames"
) as mock_map_licence_codes_to_filenames:
    mock_map_licence_codes_to_filenames.return_value = {}
    PROCESSORS = [WorkflowProcessor(), LinkProcessor()]


def test_workflows_with_only_cwl_input_valid():
    workflow_processor = [WorkflowProcessor()]
    stac_input_location = "test_data/test_workflows_with_only_cwl_input_valid.json"
    stac_expected_location = "test_data/test_workflows_with_only_cwl_input_valid-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=workflow_processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    # Check generated STAC is correct
    assert output_json == expected_json


def test_workflows_with_only_cwl_input_invalid():
    workflow_processor = [WorkflowProcessor()]
    stac_input_location = "test_data/test_workflows_with_only_cwl_input_invalid.json"
    stac_expected_location = "test_data/test_workflows_with_only_cwl_input_invalid-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=workflow_processor,
        workspace="test",
    )
    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    # Check generated stac is complete and sound
    output_keys = list(output_json.keys())
    expected_keys = list(expected_json.keys())
    assert output_keys == expected_keys

    # Check generated STAC is correct, including generated uuid
    for key in output_json:
        if key in ("id", "title"):
            id_uuid = output_json[key].rsplit("__")[1]
            uuid_obj = UUID(id_uuid, version=4)
            assert uuid_obj == uuid_obj
        else:
            assert output_json[key] == expected_json[key]


def test_workflows_dont_overwrite():
    workflow_processor = [WorkflowProcessor()]
    stac_input_location = "test_data/test_workflows_dont_overwrite.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)
        input_data = copy.deepcopy(file_json)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=workflow_processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Check generated STAC is correct with no changes to input
    assert output_json == input_data


def test_workflows_fill_blanks():
    workflow_processor = [WorkflowProcessor()]
    stac_input_location = "test_data/test_workflows_fill_blanks.json"
    stac_expected_location = "test_data/test_workflows_fill_blanks-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=workflow_processor,
        workspace="test",
    )
    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    assert output_json == expected_json


def test_workflows_correct_self_link():
    workflow_processor = [WorkflowProcessor()]
    stac_input_location = "test_data/test_workflows_correct_self_link.json"
    stac_expected_location = "test_data/test_workflows_correct_self_link-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=workflow_processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    # Check generated STAC is correct
    assert output_json == expected_json


def test_workflows_and_links_with_new_self_link():
    stac_input_location = "test_data/test_workflows_correct_self_link.json"
    stac_expected_location = "test_data/test_workflows_and_links_with_new_self_link-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=PROCESSORS,
        workspace="test",
    )
    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    # Check generated STAC is correct
    assert output_json == expected_json


def test_workflows_and_links_with_only_cwl_input_valid():
    stac_input_location = "test_data/test_workflows_with_only_cwl_input_valid.json"
    stac_expected_location = "test_data/test_workflows_and_links_with_only_cwl_input_valid-expected.json"

    # Load test STAC data
    with open(stac_input_location) as file:
        file_json = json.load(file)

    # Execute update file process
    output = update_file(
        file_name=stac_input_location,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=file_json,
        output_root=OUTPUT_ROOT,
        processors=PROCESSORS,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    with open(stac_expected_location) as file:
        expected_json = json.load(file)

    # Check generated STAC is correct
    assert output_json == expected_json

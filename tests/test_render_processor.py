import copy
import json

import pytest

from harvest_transformer.render_processor import RenderProcessor
from harvest_transformer.transformer import update_file

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


@pytest.fixture
def mock_sentinel_collection():
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "sentinel2_ard",
        "title": "",
        "description": "sentinel 2 ARD",
        "links": [],
        "keywords": [],
        "license": "sentinel",
        "providers": [],
        "extent": {
            "spatial": {"bbox": [[-9.00034454651177, 49.48562028352171, 3.1494256015866995, 61.33444247301668]]},
            "temporal": {"interval": [["2023-01-01T11:14:51.000Z", "2023-11-01T11:43:49.000Z"]]},
        },
        "summaries": {
            "Instrument Family Name Abbreviation": ["MSI"],
            "NSSDC Identifier": ["2015-000A"],
            "Start Orbit Number": ["030408", "030422", "032553", "032567"],
            "Instrument Family Name": ["Multi-Spectral Instrument"],
            "Platform Number": ["2A", "2B"],
            "Start Relative Orbit Number": ["023", "037", "066", "080", "094", "123", "137"],
            "Ground Tracking Direction": ["ascending", "descending"],
        },
        "assets": {},
    }


@pytest.fixture
def mock_file_name():
    return "mytestfile.json"


def test_is_renderable_collection__success(mock_sentinel_collection):
    processor = RenderProcessor()

    assert processor.is_renderable(mock_sentinel_collection)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        pytest.param("type", "NotACollection", id="collection"),
        pytest.param("id", "not_sentinel2_ard", id="id"),
    ],
)
def test_is_renderable_collection__failure(key, value, mock_sentinel_collection):
    mock_sentinel_collection[key] = value
    processor = RenderProcessor()

    assert processor.is_renderable(mock_sentinel_collection) is False


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("type", id="collection"),
        pytest.param("id", id="id"),
    ],
)
def test_is_sentinel2_ard_collection__failure_missing(key, mock_sentinel_collection):
    del mock_sentinel_collection[key]
    processor = RenderProcessor()

    assert processor.is_renderable(mock_sentinel_collection) is False


def test_sentinel2_ard_collection(mock_sentinel_collection, mock_file_name):
    processor = [RenderProcessor()]

    input_data = copy.deepcopy(mock_sentinel_collection)

    # Execute update file process
    output = update_file(
        file_name=mock_file_name,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=mock_sentinel_collection,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Check other data is unchanged
    for key in output_json:
        if key not in ["stac_extensions", "renders"]:
            assert output_json[key] == input_data[key]

    assert "https://stac-extensions.github.io/render/v1.0.0/schema.json" in output_json["stac_extensions"]
    assert isinstance(output_json["renders"], dict)


@pytest.mark.parametrize(
    "stac_extension",
    [
        pytest.param(None, id="none"),
        pytest.param(["items", "in", "list"], id="list"),
    ],
)
def test_sentinel2_ard_collection__missing_fields(stac_extension, mock_sentinel_collection, mock_file_name):
    processor = [RenderProcessor()]

    if stac_extension:
        mock_sentinel_collection["stac_extensions"] = stac_extension
    else:
        del mock_sentinel_collection["stac_extensions"]

    input_data = copy.deepcopy(mock_sentinel_collection)

    # Execute update file process
    output = update_file(
        file_name=mock_file_name,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=mock_sentinel_collection,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Check other data is unchanged
    for key in output_json:
        if key not in ["stac_extensions", "renders"]:
            assert output_json[key] == input_data[key]

    assert "https://stac-extensions.github.io/render/v1.0.0/schema.json" in output_json["stac_extensions"]
    assert isinstance(output_json["renders"], dict)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        pytest.param("type", "NotACollection", id="collection"),
        pytest.param("id", "not_sentinel2_ard", id="id"),
    ],
)
def test_not_sentinel2_ard_collection(key, value, mock_sentinel_collection, mock_file_name):
    processor = [RenderProcessor()]

    mock_sentinel_collection[key] = value

    input_data = copy.deepcopy(mock_sentinel_collection)

    # Execute update file process
    output = update_file(
        file_name=mock_file_name,
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=mock_sentinel_collection,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Check other data is unchanged
    for key in output_json:
        if key not in ["stac_extensions", "renders"]:
            assert output_json[key] == input_data[key]

    # Check renders fields not added
    assert "https://stac-extensions.github.io/render/v1.0.0/schema.json" not in output_json["stac_extensions"]
    assert "renders" not in output_json

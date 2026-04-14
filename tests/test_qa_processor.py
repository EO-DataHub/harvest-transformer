import copy
import json

from harvest_transformer.qa_processor import QAProcessor
from harvest_transformer.transformer import update_file

SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"
QA_ASSET_ROOT = "https://collection-qa.s3.eu-west-2.amazonaws.com"


def make_collection(collection_id="sentinel2_ard", assets=None):
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": collection_id,
        "title": "",
        "description": "test collection",
        "links": [],
        "keywords": [],
        "license": "sentinel",
        "providers": [],
        "extent": {
            "spatial": {"bbox": [[-9.0, 49.0, 3.0, 61.0]]},
            "temporal": {"interval": [["2023-01-01T00:00:00Z", "2023-12-31T00:00:00Z"]]},
        },
        "summaries": {},
        "assets": assets or {},
    }


def test_adds_qa_assets_to_mapped_collection():
    processor = [QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})]
    entry_body = make_collection()

    output = update_file(
        file_name="mytestfile.json",
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=entry_body,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    output_json = json.loads(output)

    assert output_json["assets"]["qa_documentation"] == {
        "href": f"{QA_ASSET_ROOT}/qa_documentation/sentinel-2_l1c_qa_check_quality_processes_review.json",
        "type": "application/json",
        "title": "Quality Processes Review",
    }
    assert output_json["assets"]["qa_radiometric"] == {
        "href": f"{QA_ASSET_ROOT}/qa_radiometric/sentinel-2_l1c_qa_check_radiometric_unc_all_dates.json",
        "type": "application/json",
        "title": "Radiometric Uncertainty",
    }


def test_leaves_unmapped_collection_unchanged():
    processor = [QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})]
    entry_body = make_collection(collection_id="sentinel1")
    input_data = copy.deepcopy(entry_body)

    output = update_file(
        file_name="mytestfile.json",
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=entry_body,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    assert json.loads(output) == input_data


def test_leaves_non_collection_unchanged():
    processor = QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})
    entry_body = {"type": "Item", "id": "example-item", "assets": {}}

    assert processor.update_file("test.json", SOURCE_PATH, OUTPUT_ROOT, entry_body, OUTPUT_ROOT) == entry_body
    assert processor.update_file("test.json", SOURCE_PATH, OUTPUT_ROOT, "raw text", OUTPUT_ROOT) == "raw text"


def test_preserves_existing_assets_when_adding_qa_assets():
    processor = [QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})]
    entry_body = make_collection(assets={"thumbnail": {"href": "https://example.com/thumb.png"}})

    output = update_file(
        file_name="mytestfile.json",
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=entry_body,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    output_json = json.loads(output)

    assert output_json["assets"]["thumbnail"] == {"href": "https://example.com/thumb.png"}
    assert "qa_documentation" in output_json["assets"]
    assert "qa_radiometric" in output_json["assets"]


def test_does_not_overwrite_existing_qa_assets():
    processor = [QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})]
    entry_body = make_collection(assets={"qa_documentation": {"href": "https://example.com/existing-doc.json"}})

    output = update_file(
        file_name="mytestfile.json",
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=entry_body,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    output_json = json.loads(output)

    assert output_json["assets"]["qa_documentation"] == {"href": "https://example.com/existing-doc.json"}
    assert "qa_radiometric" in output_json["assets"]


def test_normalizes_asset_root_trailing_slash():
    processor = [QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"}, asset_root="https://qa.example.test///")]
    entry_body = make_collection()

    output = update_file(
        file_name="mytestfile.json",
        source=SOURCE_PATH,
        target_location=OUTPUT_ROOT + TARGET,
        entry_body=entry_body,
        output_root=OUTPUT_ROOT,
        processors=processor,
        workspace="test",
    )

    output_json = json.loads(output)

    assert output_json["assets"]["qa_documentation"]["href"].startswith("https://qa.example.test/qa_documentation/")
    assert "//qa_documentation/" not in output_json["assets"]["qa_documentation"]["href"]
    assert output_json["assets"]["qa_radiometric"]["href"].startswith("https://qa.example.test/qa_radiometric/")
    assert "//qa_radiometric/" not in output_json["assets"]["qa_radiometric"]["href"]


def test_leaves_collection_without_type_or_id_unchanged():
    processor = QAProcessor({"sentinel2_ard": "sentinel-2_l1c_qa"})
    entry_without_id = {"type": "Collection", "assets": {}}
    entry_without_type = {"id": "sentinel2_ard", "assets": {}}

    assert (
        processor.update_file("test.json", SOURCE_PATH, OUTPUT_ROOT, entry_without_id, OUTPUT_ROOT) == entry_without_id
    )
    assert (
        processor.update_file("test.json", SOURCE_PATH, OUTPUT_ROOT, entry_without_type, OUTPUT_ROOT)
        == entry_without_type
    )

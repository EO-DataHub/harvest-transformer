import json

from harvest_transformer.file_processor import FileProcessor

STAC_LOCATION = "test_data/dataset-test-stac.json"
SOURCE_PATH = "https://example.link.for.test/"
TARGET = "/target_directory/"
OUTPUT_ROOT = "https://output.root.test"


def test_links_replacement():
    file_processor = FileProcessor()
    # Load test STAC data
    with open(STAC_LOCATION, "r") as file:
        json_data = json.load(file)

    # Execute update file process
    output = file_processor.update_file(STAC_LOCATION, SOURCE_PATH, TARGET, json_data, OUTPUT_ROOT)

    # Read output in as a dictionary
    output_json = json.loads(output)

    # Expected outputs
    expect_self_link = "https://output.root.test/target_directory/collections/sentinel2_ard/items/neodc.sentinel_ard.data.sentinel_2.2023.11.20.S2A_20231120_latn501lonw0036_T30UVA_ORB037_20231120132420_utm30n_osgb"
    expect_root_link = "https://output.root.test/target_directory/"
    expect_parent_link = "https://output.root.test/target_directory/collections/sentinel2_ard"
    expect_collection_link = "https://output.root.test/target_directory/collections/sentinel2_ard"

    # Check updates links are correct
    for link in output_json["links"]:
        if link["rel"] == "self":
            self_link = link
        elif link["rel"] == "root":
            root_link = link
        elif link["rel"] == "parent":
            parent_link = link
        elif link["rel"] == "collection":
            collection_link = link

    assert self_link["href"] == expect_self_link
    assert root_link["href"] == expect_root_link
    assert parent_link["href"] == expect_parent_link
    assert collection_link["href"] == expect_collection_link

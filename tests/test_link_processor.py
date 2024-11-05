import copy
import json
import unittest

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


class TestLinkProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = LinkProcessor()

    def test_add_new_license_link(self):
        json_data = {"links": []}
        self.processor.add_license_link(
            json_data,
            "license",
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )
        self.assertEqual(len(json_data["links"]), 1)
        self.assertEqual(json_data["links"][0]["rel"], "license")
        self.assertEqual(
            json_data["links"][0]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )

    def test_avoid_duplicate_license_link(self):
        json_data = {
            "links": [
                {
                    "rel": "license",
                    "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
                }
            ]
        }
        self.processor.add_license_link(
            json_data,
            "license",
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )
        self.assertEqual(len(json_data["links"]), 1)
        self.assertEqual(json_data["links"][0]["rel"], "license")
        self.assertEqual(
            json_data["links"][0]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )

    def test_add_multiple_license_links(self):
        json_data = {
            "links": [
                {
                    "rel": "license",
                    "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
                }
            ]
        }
        self.processor.add_license_link(
            json_data,
            "license",
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/html/APSL-1.2.html",
        )
        self.assertEqual(len(json_data["links"]), 2)
        self.assertEqual(json_data["links"][0]["rel"], "license")
        self.assertEqual(
            json_data["links"][0]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )
        self.assertEqual(json_data["links"][1]["rel"], "license")
        self.assertEqual(
            json_data["links"][1]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/html/APSL-1.2.html",
        )

    def test_add_license_link_to_existing_links(self):
        json_data = {
            "links": [
                {"rel": "self", "href": "https://example.com/self"},
                {"rel": "parent", "href": "https://example.com/parent"},
            ]
        }
        self.processor.add_license_link(
            json_data,
            "license",
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )
        self.assertEqual(len(json_data["links"]), 3)
        self.assertEqual(json_data["links"][2]["rel"], "license")
        self.assertEqual(
            json_data["links"][2]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )

    def test_avoid_duplicate_license_links_among_existing_links(self):
        json_data = {
            "links": [
                {"rel": "self", "href": "https://example.com/self"},
                {"rel": "parent", "href": "https://example.com/parent"},
                {
                    "rel": "license",
                    "href": "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
                },
            ]
        }
        self.processor.add_license_link(
            json_data,
            "license",
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )
        self.assertEqual(len(json_data["links"]), 3)
        self.assertEqual(json_data["links"][2]["rel"], "license")
        self.assertEqual(
            json_data["links"][2]["href"],
            "https://dev.eodatahub.org.uk/harvested/default/spdx/license-list-data/main/text/APL-1.0.txt",
        )


if __name__ == "__main__":
    unittest.main()

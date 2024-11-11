import logging
import os
from typing import Union
from urllib.parse import urljoin, urlparse

import boto3

from .utils import SPDXLicenseError
from .workflow_processor import WorkflowProcessor

# Create workflow processor for generating workflow STAC definitions from CWL
workflow_stac_processor = WorkflowProcessor()


class LinkProcessor:
    spdx_license_path = "harvested/default/spdx/license-list-data/main/"
    spdx_license_list = []  # This will be populated with the list of valid SPDX IDs

    def __init__(self):
        # Populate the SPDX_LICENSE_LIST with valid SPDX IDs
        self.hosted_zone = os.getenv("HOSTED_ZONE")
        self.spdx_bucket_name = os.getenv("S3_BUCKET")
        self.spdx_license_list = self.list_s3_license_files(
            bucket_name=self.spdx_bucket_name, prefix=self.spdx_license_path + "html/"
        )

    def list_s3_license_files(self, bucket_name, prefix):
        # Initialize an S3 client
        s3 = boto3.client("s3")
        logging.info(f"Bucket name {bucket_name} and prefix {prefix}")
        # List objects within the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=10000)

        # Extract file names without the extension
        if "Contents" in response:
            files = [
                obj["Key"].split("/")[-1].rsplit(".", 1)[0]
                for obj in response["Contents"]
                if obj["Key"].endswith(".html")
            ]
            return files
        else:
            raise SPDXLicenseError(
                f"No html license files found in {bucket_name} with prefix {prefix}"
            )

    def is_valid_url(self, url: str) -> bool:
        """Checks if a given URL is valid"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def delete_sections(self, stac_data: dict) -> dict:
        """Remove all unnecessary data from a file."""
        stac_data.pop("conformsTo", None)
        return stac_data

    def find_all_links(self, node):
        """Recursively find all nested links in a given item"""
        if isinstance(node, list):
            for i in node:
                yield from self.find_all_links(i)
        elif isinstance(node, dict):
            if "links" in node:
                yield from node["links"]
            for j in node.values():
                yield from self.find_all_links(j)

    def rewrite_links(
        self, stac_data: dict, source: str, target_location: str, output_self: str, output_root: str
    ) -> dict:
        """Rewrite links so that they are suitable for an EODHP catalogue"""
        for link in self.find_all_links(stac_data):
            if not link.get("href").startswith(output_root):
                if link.get("href").startswith(source):
                    # Link is an absolute link. Replace the source.
                    link["href"] = link["href"].replace(source, target_location)
                elif link.get("rel") == "parent":
                    link["href"] = output_self.rsplit("/", 2)[0]
                elif link.get("href").startswith("http"):
                    # Link is an absolute link with a different root than expected.
                    # Assume valid link outside of EODH
                    pass
                else:
                    # Link is a relative link. Convert to absolute link.
                    link["href"] = urljoin(output_self.rsplit("/", 1)[0], link.get("href"))
        return stac_data

    def add_missing_links(self, stac_data: dict, new_root: str, new_self: str) -> dict:
        """As per STAC best practices, ensure root and self links exist."""

        self.add_link_if_missing(stac_data, "root", new_root)
        self.add_link_if_missing(stac_data, "self", new_self)

        return stac_data

    def add_link_if_missing(self, stac_data: dict, rel: str, href: str):
        """Ensures a link consisting of given rel exists in links."""
        links = stac_data.get("links")
        link_exists = False
        if not links:
            stac_data.update({"links": [{"rel": rel, "href": href}]})
            return
        for link in links:
            if link.get("rel") == rel:
                link_exists = True
        if not link_exists:
            links.append({"rel": rel, "href": href})

    def add_license_link(self, stac_data: dict, href: str):
        """Ensures unique license links, overwriting if already present."""
        links = stac_data.setdefault("links", [])
        link_type = "text/plain" if href.endswith(".txt") else "text/html"
        links.append({"rel": "license", "href": href, "type": link_type})

    def ensure_license_links(self, stac_data: dict):
        """Ensure that valid SPDX license links are present."""
        links = stac_data.get("links", [])
        # Check whether license field is provided
        license_field = stac_data.get("license")
        if not license_field:
            return
        # If a license link already exists, do not add new ones
        for link in links:
            if link.get("rel") == "license":
                return
        license_field_case_insensitive = license_field.upper()
        # Check whether license field is a valid SPDX ID
        found_license = ""
        for license_id in self.spdx_license_list:
            if license_id.upper() == license_field_case_insensitive:
                found_license = license_id
                break
        if not found_license:
            return

        base_url = f"https://{self.hosted_zone}/{self.spdx_license_path}"

        text_url = urljoin(base_url + "/text/", f"{found_license}.txt")
        html_url = urljoin(base_url + "/html/", f"{found_license}.html")

        self.add_license_link(stac_data, text_url)
        self.add_license_link(stac_data, html_url)

    def update_file(
        self,
        file_name: str,
        source: str,
        target_location: str,
        file_body: Union[dict, str],
        output_root: str,
        **kwargs,
    ) -> dict:
        """
        Updates content within a given file name. File name may either be a URL or S3 key.
        Uploads updated file contents to updated_key within the given bucket.
        """

        # Only concerned with STAC data here, other files can be uploaded as is
        if not isinstance(file_body, dict):
            return file_body

        # Delete unnecessary sections
        file_body = self.delete_sections(file_body)
        try:
            self_link = [
                link.get("href") for link in file_body.get("links") if link.get("rel") == "self"
            ][0]
        except (TypeError, IndexError):
            logging.info(f"File {file_name} does not contain a self link. Adding temporary link.")
            # Create temporary self link in item using source which will be replaced by the subsequent
            # transformer
            self.add_link_if_missing(file_body, "self", source + file_name)
            self_link = [
                link.get("href") for link in file_body.get("links") if link.get("rel") == "self"
            ][0]

        output_self = self_link.replace(source, target_location)
        if not self.is_valid_url(output_self):
            logging.error(
                f"File {file_name} does not produce a valid self link with given "
                f"self link {self_link}, source {source}, and target {target_location}. "
                f"Unable to rewrite links."
            )
            return file_body

        # Update links to STAC best practices
        file_body = self.add_missing_links(file_body, output_root, output_self)

        # Ensure SPDX license links are present
        self.ensure_license_links(file_body)

        # Update links to refer to EODH
        file_body = self.rewrite_links(file_body, source, target_location, output_self, output_root)

        # Return json for further transform and upload
        return file_body

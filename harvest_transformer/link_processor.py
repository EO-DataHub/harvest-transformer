import logging
import os
from typing import Union
from urllib.parse import urljoin, urlparse

import boto3
import requests
from html_sanitizer import Sanitizer

from .utils import SPDXLicenseError
from .workflow_processor import WorkflowProcessor

# Create workflow processor for generating workflow STAC definitions from CWL
workflow_stac_processor = WorkflowProcessor()


class LinkProcessor:
    spdx_license_path = "api/catalogue/licences/spdx/"
    spdx_license_list = []  # This will be populated with the list of valid SPDX IDs

    def __init__(self, workspace: str):
        self.workspace = workspace
        # Populate the SPDX_LICENSE_LIST with valid SPDX IDs
        self.hosted_zone = os.getenv("HOSTED_ZONE")
        self.spdx_bucket_name = os.getenv("S3_BUCKET")
        self.spdx_license_dict = self.map_licence_codes_to_filenames(
            bucket_name=self.spdx_bucket_name, prefix=self.spdx_license_path + "html/"
        )
        # Initialize S3 client
        self.s3_client = boto3.client("s3")
        self.sanitizer = Sanitizer()

    def map_licence_codes_to_filenames(self, bucket_name, prefix) -> dict[str, str]:
        # Initialize an S3 client
        s3 = boto3.client("s3")
        logging.info(f"Bucket name {bucket_name} and prefix {prefix}")
        # List objects within the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=10000)

        files = {
            obj["Key"]
            .split("/")[-1]
            .rsplit(".", 1)[0]
            .casefold(): obj["Key"]
            .split("/")[-1]
            .rsplit(".", 1)[0]
            for obj in response.get("Contents", [])
            if obj.get("Key", "").endswith(".html")
        }

        if files:
            return files
        else:
            logging.info(f"No html license files found in {bucket_name} with prefix {prefix}")
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
        relations_to_rewrite = ["child", "collection", "item", "items", "parent", "root", "self"]
        # Remove relations that refer to api describing the original catalogue
        relations_to_remove = [
            "aggregate",
            "aggregations",
            "queryables",
            "http:/www.opengis.net/def/rel/ogc/1.0/queryables",
            "search",
            "service-desc",
            "service-doc",
        ]

        new_links = []

        for link in self.find_all_links(stac_data):
            href = link.get("href")
            rel = link.get("rel")

            if href.startswith(output_root):
                # Link is already EODH link
                new_links.append(link)
                continue

            parsed_href = urlparse(href)

            if rel in relations_to_rewrite:
                if href.startswith(source):
                    # Link is an absolute link. Replace the source.
                    link["href"] = href.replace(source, target_location)
                elif rel == "parent":
                    # Link is a parent link. Path to parent via self link.
                    link["href"] = output_self.rsplit("/", 2)[0]
                elif href.startswith(output_root.strip("/")):
                    # Link is an EODH link. Do nothing.
                    pass
                elif not parsed_href.scheme and not parsed_href.netloc:
                    # Link is a relative link. Convert to absolute link.
                    link["href"] = urljoin(output_self, href)
                else:
                    # Link cannot be rewritten and should not be external. Drop it.
                    continue
            elif rel in relations_to_remove:
                # Drop links that we know are not relevant to the EODH harvested catalogue
                continue

            # Keep links by default
            new_links.append(link)

        stac_data["links"] = new_links
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
        # Check whether license field is a valid SPDX ID
        found_license = self.spdx_license_dict.get(license_field.casefold())
        if found_license:
            base_url = f"https://{self.hosted_zone}/{self.spdx_license_path}"

            text_url = urljoin(base_url + "/text/", f"{found_license}.txt")
            html_url = urljoin(base_url + "/html/", f"{found_license}.html")
            # Check if the URLs are already present in the links
            if not any(link.get("href") == text_url for link in links):
                self.add_license_link(stac_data, text_url)
            if not any(link.get("href") == html_url for link in links):
                self.add_license_link(stac_data, html_url)
        else:
            # If a license link already exists, do not add new ones
            for link in links:
                if link.get("rel") == "license":
                    href = link.get("href")
                    if not href.startswith(f"https://{self.hosted_zone}"):
                        # Copy the license file to EODH public bucket and update the link
                        new_href = self.copy_license_to_eodh(href)
                        link["href"] = new_href

    def update_file(
        self,
        file_name: str,
        source: str,
        target_location: str,
        entry_body: Union[dict, str],
        output_root: str,
        **kwargs,
    ) -> dict:
        """
        Updates content within a given file name. File name may either be a URL or S3 key.
        Uploads updated file contents to updated_key within the given bucket.
        """

        # Only concerned with STAC data here, other files can be uploaded as is
        if not isinstance(entry_body, dict):
            return entry_body

        # Delete unnecessary sections
        entry_body = self.delete_sections(entry_body)
        try:
            self_link = [
                link.get("href") for link in entry_body.get("links") if link.get("rel") == "self"
            ][0]
        except (TypeError, IndexError):
            logging.info(f"File {file_name} does not contain a self link. Adding temporary link.")
            # Create temporary self link in item using source which will be replaced by the subsequent
            # transformer
            self.add_link_if_missing(entry_body, "self", source + file_name)
            self_link = [
                link.get("href") for link in entry_body.get("links") if link.get("rel") == "self"
            ][0]

        output_self = self_link.replace(source, target_location, 1)
        if not self.is_valid_url(output_self):
            logging.error(
                f"File {file_name} does not produce a valid self link with given "
                f"self link {self_link}, source {source}, and target {target_location}. "
                f"Unable to rewrite links."
            )
            return entry_body

        # Update links to STAC best practices
        entry_body = self.add_missing_links(entry_body, output_root, output_self)

        # Ensure SPDX license links are present
        self.ensure_license_links(entry_body)

        # Update links to refer to EODH
        entry_body = self.rewrite_links(
            entry_body, source, target_location, output_self, output_root
        )

        # Return json for further transform and upload
        return entry_body

    def copy_license_to_eodh(self, href: str) -> str:
        """Copy the license file to the EODH public bucket and return the new URL."""
        # Download the license file
        response = requests.get(href)
        response.raise_for_status()
        # Sanitize HTML if necessary
        content_type = response.headers.get("Content-Type", "")
        content = response.content
        if "text/html" in content_type:
            content = self.sanitizer.sanitize(response.text).encode("utf-8")

        # Determine the new filename and path
        filename = href.split("/")[-1]
        new_path = f"api/catalogue/licences/{self.workspace}/{filename}"

        # Check if the file already exists in the bucket
        try:
            self.s3_client.head_object(Bucket=self.spdx_bucket_name, Key=new_path)
            # If the file exists, return the existing URL
            return f"https://{self.hosted_zone}/{new_path}"
        except self.s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        # Upload the file to the EODH public bucket
        self.s3_client.put_object(
            Bucket=self.spdx_bucket_name,
            Key=new_path,
            Body=content,
            ContentType=content_type,
        )
        # Return the new URL
        return f"https://{self.hosted_zone}/{new_path}"

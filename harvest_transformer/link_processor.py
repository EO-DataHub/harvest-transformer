import logging
from typing import Union
from urllib.parse import urljoin, urlparse

from .workflow_processor import WorkflowProcessor

# Create workflow processor for generating workflow STAC definitions from CWL
workflow_stac_processor = WorkflowProcessor()


class LinkProcessor:
    def is_valid_url(self, url: str) -> bool:
        """Checks if a given URL is valid"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def delete_sections(self, json_data: dict) -> dict:
        """Remove all unnecessary data from a file."""
        json_data.pop("conformsTo", None)
        return json_data

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
        self, json_data: dict, source: str, target_location: str, output_self: str, output_root: str
    ) -> dict:
        """Rewrite links so that they are suitable for an EODHP catalogue"""
        relations_to_rewrite = ["child", "collection", "item", "items", "parent", "root", "self"]
        relations_to_keep = [
            "about",
            "author",
            "cite-as",
            "copyright",
            "external",
            "license",
            "lrdd",
            "service",
            "service-desc",
            "service-doc",
            "service-meta",
            "thumbnail",
            "via",
        ]

        new_links = []

        for link in self.find_all_links(json_data):
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
                    link["href"] = urljoin(output_self.rsplit("/", 1)[0], href)
                else:
                    # Link cannot be rewritten and should not be external. Drop it.
                    continue
                new_links.append(link)
            elif rel in relations_to_keep:
                new_links.append(link)

        json_data["links"] = new_links
        return json_data

    def add_missing_links(self, json_data: dict, new_root: str, new_self: str) -> dict:
        """As per STAC best practices, ensure root and self links exist."""

        self.add_link_if_missing(json_data, "root", new_root)
        self.add_link_if_missing(json_data, "self", new_self)

        return json_data

    def add_link_if_missing(self, json_data: dict, rel: str, href: str):
        """Ensures a link consisting of given rel exists in links."""
        links = json_data.get("links")
        link_exists = False
        if not links:
            json_data.update({"links": [{"rel": rel, "href": href}]})
            return
        for link in links:
            if link.get("rel") == rel:
                link_exists = True
        if not link_exists:
            links.append({"rel": rel, "href": href})

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

        output_self = self_link.replace(source, target_location, 1)
        if not self.is_valid_url(output_self):
            logging.error(
                f"File {file_name} does not produce a valid self link with given "
                f"self link {self_link}, source {source}, and target {target_location}. "
                f"Unable to rewrite links."
            )
            return file_body

        # Update links to STAC best practices
        file_body = self.add_missing_links(file_body, output_root, output_self)

        # Update links to refer to EODH
        file_body = self.rewrite_links(file_body, source, target_location, output_self, output_root)

        # Return json for further transform and upload
        return file_body

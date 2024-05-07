import logging
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
                for x in self.find_all_links(i):
                    yield x
        elif isinstance(node, dict):
            if "links" in node:
                for link in node["links"]:
                    yield link
            for j in node.values():
                for x in self.find_all_links(j):
                    yield x

    def rewrite_links(
        self, json_data: dict, source: str, target_location: str, output_self: str, output_root: str
    ) -> dict:
        """Rewrite links so that they are suitable for an EODHP catalogue"""
        for link in self.find_all_links(json_data):
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
        file_json: dict,
        output_root: str,
    ) -> dict:
        """
        Updates content within a given file name. File name may either be a URL or S3 key.
        Uploads updated file contents to updated_key within the given bucket.
        """

        # Delete unnecessary sections
        file_json = self.delete_sections(file_json)
        try:
            self_link = [
                link.get("href") for link in file_json.get("links") if link.get("rel") == "self"
            ][0]
        except (TypeError, IndexError):
            logging.info(f"File {file_name} does not contain a self link. Adding temporary link.")
            # Create temporary self link in item using source which will be replaced by the subsequent
            # transformer
            self.add_link_if_missing(file_json, "self", source + file_name)
            self_link = [
                link.get("href") for link in file_json.get("links") if link.get("rel") == "self"
            ][0]

        output_self = self_link.replace(source, target_location)
        if not self.is_valid_url(output_self):
            logging.error(
                f"File {file_name} does not produce a valid self link with given "
                f"self link {self_link}, source {source}, and target {target_location}. "
                f"Unable to rewrite links."
            )
            return file_json

        # Update links to STAC best practices
        file_json = self.add_missing_links(file_json, output_root, output_self)

        # Update links to refer to EODH
        file_json = self.rewrite_links(file_json, source, target_location, output_self, output_root)

        # Return json for further transform and upload
        return file_json

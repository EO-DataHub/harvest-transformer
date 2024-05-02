import json
import logging

from .link_processor import LinkProcessor
from .workflow_processor import WorkflowProcessor

# Create workflow processor for generating workflow STAC definitions from CWL
workflow_processor = WorkflowProcessor()

# Create link processor for updating harvested links
link_processor = LinkProcessor()


class FileProcessor:

    def update_file(
        self,
        file_name: str,
        source: str,
        target: str,
        file_json: dict,
        output_root: str,
    ) -> str:
        """
        Updates content within a given file name. File name may either be a URL or S3 key.
        Uploads updated file contents to updated_key within the given bucket.
        """
        target_location = output_root + target

        # Generate STAC collection definition for workflows
        if "assets" in file_json and "cwl_script" in file_json["assets"]:
            file_json = workflow_processor.update_file(file_name, source, file_json)
            logging.info(f"Workflow STAC collection successfully rewritten for file {file_name}")

        file_json = link_processor.update_file(
            file_name, source, target_location, file_json, output_root
        )

        # Convert json to string for file upload
        file_body = json.dumps(file_json)
        return file_body

import json
import logging
import os
from typing import Sequence, Union

from eodhp_utils.messagers import (
    CatalogueChangeBodyMessager,
    CatalogueChangeMessager,
    Messager,
)

from .transformer import transform, transform_key


class TransformerMessager(CatalogueChangeBodyMessager):
    def process_update(
        self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
    ) -> Sequence[Messager.Action]:
        get_result = self.s3_client.get_object(Bucket=input_bucket, Key=input_key)
        entry_body = get_result["Body"].read()
        # Transformer needs updating to ensure that content type is set to this
        # if get_result["ResponseMetadata"]["HTTPHeaders"]["content-type"] == "application/json":
        try:
            entry_body = json.loads(entry_body)
        except ValueError:
            # Not a JSON file - consume it as a string
            logging.info(f"File {input_key} is not valid JSON.")
        return self.process_update_body(entry_body, cat_path, source, target)

    def process_update_body(
        self, entry_body: Union[dict, str], cat_path: str, source: str, target: str
    ) -> Sequence[CatalogueChangeMessager.Action]:
        workspace_from_msg = self.get_workspace_from_msg()
        output_root = os.getenv("OUTPUT_ROOT")
        entry_body = transform(
            file_name=cat_path,
            entry_body=entry_body,
            source=source,
            target=target,
            output_root=output_root,
            workspace=workspace_from_msg,
        )
        updated_key = transform_key(cat_path, source, target)
        return [Messager.OutputFileAction(file_body=entry_body, cat_path=updated_key)]

    def process_delete(
        self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
    ) -> Sequence[Messager.Action]:
        # Calculated updated key
        updated_key = transform_key(cat_path, source, target)
        # Action to remove file from S3
        return [Messager.OutputFileAction(file_body=None, cat_path=updated_key)]

    def get_workspace_from_msg(self):
        return self.input_change_msg.get("workspace")

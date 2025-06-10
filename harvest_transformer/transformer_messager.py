import os
from typing import Sequence, Union

from eodhp_utils.messagers import (
    CatalogueChangeBodyMessager,
    CatalogueChangeMessager,
    Messager,
)
from pulsar import Message

from .transformer import transform, transform_key


class TransformerMessager(CatalogueChangeBodyMessager):
    def __init__(self, processors, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processors = processors

    def process_update_body(
        self, entry_body: Union[dict, str], cat_path: str, source: str, target: str
    ) -> Sequence[CatalogueChangeMessager.Action]:
        workspace_from_msg = self.get_workspace_from_msg()
        output_root = os.getenv("OUTPUT_ROOT")
        entry_body = transform(
            processors=self.processors,
            file_name=cat_path,
            entry_body=entry_body,
            source=source,
            target=target,
            output_root=output_root,
            workspace=workspace_from_msg,
            s3_client=self.s3_client,
        )
        updated_key = transform_key(cat_path, source, target)
        return [
            Messager.OutputFileAction(
                file_body=entry_body,
                bucket=self.input_change_msg.get("bucket_name"),
                cat_path=updated_key,
            )
        ]

    def process_delete(
        self, input_bucket: str, input_key: str, cat_path: str, source: str, target: str
    ) -> Sequence[Messager.Action]:
        # Calculated updated key
        updated_key = transform_key(cat_path, source, target)
        # Action to remove file from S3
        return [Messager.OutputFileAction(file_body=None, cat_path=updated_key)]

    def get_workspace_from_msg(self):
        return self.input_change_msg.get("workspace")

    def gen_empty_catalogue_message(self, msg: Message) -> dict:
        """
        Generate an empty catalogue change message without updated_keys, deleted_keys, or added_keys.
        """
        return {
            "id": self.input_change_msg.get("id"),
            "workspace": self.input_change_msg.get("workspace"),
            "bucket_name": self.input_change_msg.get("bucket_name"),
            "source": self.input_change_msg.get("source"),
            "target": self.input_change_msg.get("target"),
        }

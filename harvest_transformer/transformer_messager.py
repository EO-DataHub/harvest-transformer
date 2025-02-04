import os
from typing import Sequence, Union

from eodhp_utils.messagers import (
    CatalogueChangeBodyMessager,
    CatalogueChangeMessager,
    Messager,
)

from .transformer import transform, transform_key


class TransformerMessager(CatalogueChangeBodyMessager):
    def process_update_body(
        self, entry_body: Union[dict, str], cat_path: str, source: str, target: str
    ) -> Sequence[CatalogueChangeMessager.Action]:
        workspace_from_msg = self.get_workspace_from_msg()
        output_root = os.getenv("OUTPUT_ROOT")
        entry_body = transform(
            self=self,
            file_name=cat_path,
            entry_body=entry_body,
            source=source,
            target=target,
            output_root=output_root,
            workspace=workspace_from_msg,
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

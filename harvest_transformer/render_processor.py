import logging
import os
from typing import Union


class RenderProcessor:
    def is_renderable(self, file_body):
        return file_body.get("type") == "Collection" and file_body.get("id") in os.environ.get(
            "RENDERABLE_COLLECTIONS"
        ).split(",")

    def add_missing_fields(self, file_body):
        if not file_body.get("stac_extensions"):
            file_body["stac_extensions"] = []

        return file_body

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

        if self.is_renderable(file_body):
            logging.info(f"{file_name} is a Renderable Collection file")
            file_body = self.add_missing_fields(file_body)

            render_extension_url = "https://stac-extensions.github.io/render/v1.0.0/schema.json"

            if render_extension_url not in file_body["stac_extensions"]:
                file_body["stac_extensions"].append(render_extension_url)

            file_body["renders"] = {
                "rgb": {
                    "title": "RGB",
                    "assets": ["cog"],
                    "bidx": [1, 2, 3],
                    "rescale": [[0, 100], [0, 100], [0, 100]],
                    "resampling": "nearest",
                    "tilematrixsets": {"WebMercatorQuad": [0, 30]},
                }
            }

        # Return json for further transform and upload
        return file_body

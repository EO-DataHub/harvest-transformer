import logging
from typing import Union

renderable_collections = {
    "sentinel2_ard": {
        "rgb": {
            "title": "RGB",
            "assets": ["cog"],
            "bidx": [1, 2, 3],
            "rescale": [[0, 100], [0, 100], [0, 100]],
            "resampling": "nearest",
            "tilematrixsets": {"WebMercatorQuad": [0, 30]},
        }
    }
}


class RenderProcessor:
    def is_renderable(self, entry_body: dict) -> bool:
        """Check to see if file describes a collection that is renderable"""
        return (
            entry_body.get("type") == "Collection"
            and entry_body.get("id") in renderable_collections.keys()
        )

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

        if self.is_renderable(entry_body):
            logging.info(f"{file_name} is a Renderable Collection file")
            entry_body.setdefault("stac_extensions", [])

            render_extension_url = "https://stac-extensions.github.io/render/v1.0.0/schema.json"

            if render_extension_url not in entry_body["stac_extensions"]:
                entry_body["stac_extensions"].append(render_extension_url)

            entry_body["renders"] = renderable_collections[entry_body.get("id")]

        # Return json for further transform and upload
        return entry_body

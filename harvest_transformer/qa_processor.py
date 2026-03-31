import logging
from typing import Union

DEFAULT_QA_ASSET_ROOT = (
    "https://npl.eodatahub-workspaces.org.uk/files/workspaces-eodhp/processing-results/qa-workflow"
)


class QAProcessor:
    def __init__(self, collection_map: dict[str, str] | None = None, asset_root: str | None = None):
        self.collection_map = collection_map or {}
        self.asset_root = (asset_root or DEFAULT_QA_ASSET_ROOT).rstrip("/")

    def is_qa_enabled_collection(self, entry_body: dict) -> bool:
        return entry_body.get("type") == "Collection" and entry_body.get("id") in self.collection_map

    def build_qa_assets(self, collection_id: str) -> dict[str, dict]:
        qa_key = self.collection_map[collection_id]
        base_href = f"{self.asset_root}/{qa_key}"
        return {
            "qa_documentation": {
                "href": (
                    f"{base_href}/qa_documentation/{qa_key}_check_quality_processes_review.json"
                ),
                "type": "application/json",
                "title": "Quality Processes Review",
            },
            "qa_radiometric": {
                "href": f"{base_href}/qa_radiometric/{qa_key}_check_radiometric_unc_all_dates.json",
                "type": "application/json",
                "title": "Radiometric Uncertainty",
            },
        }

    def upsert_asset(self, entry_body: dict, key: str, asset_def: dict):
        entry_body.setdefault("assets", {})
        if key not in entry_body["assets"]:
            entry_body["assets"][key] = asset_def

    def update_file(
        self,
        file_name: str,
        source: str,
        target_location: str,
        entry_body: Union[dict, str],
        output_root: str,
        **kwargs,
    ) -> dict:
        del file_name, source, target_location, output_root, kwargs
        if not isinstance(entry_body, dict):
            return entry_body

        if not self.is_qa_enabled_collection(entry_body):
            return entry_body

        collection_id = entry_body["id"]
        logging.info(f"Adding QA assets to collection {collection_id}")
        for key, asset_def in self.build_qa_assets(collection_id).items():
            self.upsert_asset(entry_body, key, asset_def)

        return entry_body

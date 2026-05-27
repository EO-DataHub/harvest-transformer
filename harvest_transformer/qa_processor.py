import logging
import time

from harvest_transformer.utils import load_json_url, url_exists

DEFAULT_QA_ASSET_ROOT = "https://collection-qa.s3.eu-west-2.amazonaws.com"
_CACHE_TTL = 60  # seconds — re-fetch map at most once per minute


class QAProcessor:
    def __init__(
        self,
        collection_map: dict[str, str] | None = None,
        map_url: str | None = None,
        asset_root: str | None = None,
    ) -> None:
        self._static_map = collection_map  # used when no URL (e.g. tests)
        self.map_url = map_url
        self.asset_root = (asset_root or DEFAULT_QA_ASSET_ROOT).rstrip("/")
        self._cached_map: dict[str, str] | None = None
        self._cache_loaded_at: float | None = None

    def _get_collection_map(self) -> dict[str, str]:
        if not self.map_url:
            return self._static_map or {}
        now = time.monotonic()
        if self._cached_map is None or self._cache_loaded_at is None or (now - self._cache_loaded_at) > _CACHE_TTL:
            try:
                fetched = load_json_url(self.map_url)
                if not isinstance(fetched, dict):
                    raise TypeError("QA collection map must be a JSON object")
                self._cached_map = fetched
                self._cache_loaded_at = now
            except (OSError, ValueError, TypeError) as exc:
                logging.warning(f"Unable to load QA collection map from {self.map_url}: {exc}")
                if self._cached_map is None:
                    self._cached_map = {}
        return self._cached_map

    def is_qa_enabled_collection(self, entry_body: dict) -> bool:
        return entry_body.get("type") == "Collection" and bool(entry_body.get("id"))

    def build_qa_assets(self, collection_id: str) -> dict[str, dict]:
        qa_key = self._get_collection_map().get(collection_id, collection_id)
        return {
            "qa_documentation": {
                "href": (f"{self.asset_root}/qa_documentation/{qa_key}_qa_check_quality_processes_review.json"),
                "type": "application/json",
                "title": "Quality Processes Review",
                "roles": ["metadata", "quality"],
            },
            "qa_radiometric": {
                "href": (f"{self.asset_root}/qa_radiometric/{qa_key}_qa_check_radiometric_unc_all_dates.json"),
                "type": "application/json",
                "title": "Radiometric Uncertainty",
                "roles": ["metadata", "quality"],
            },
        }

    def upsert_asset(self, entry_body: dict, key: str, asset_def: dict) -> None:
        entry_body.setdefault("assets", {})
        if key not in entry_body["assets"]:
            entry_body["assets"][key] = asset_def

    def update_file(
        self,
        file_name: str,
        source: str,
        target_location: str,
        entry_body: dict | str,
        output_root: str,
        **kwargs: object,
    ) -> dict | str:
        del file_name, source, target_location, output_root, kwargs
        if not isinstance(entry_body, dict):
            return entry_body

        if not self.is_qa_enabled_collection(entry_body):
            return entry_body

        collection_id = entry_body["id"]
        logging.info(f"Checking QA assets for collection {collection_id}")
        for key, asset_def in self.build_qa_assets(collection_id).items():
            if url_exists(asset_def["href"]):
                logging.info(f"Adding {key} asset to collection {collection_id}")
                self.upsert_asset(entry_body, key, asset_def)
            else:
                logging.debug(f"QA asset not found, skipping {key} for {collection_id}: {asset_def['href']}")

        return entry_body

harvest_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "bucket_name": {"type": "string"},
        "added_keys": {"type": "array"},
        "updated_keys": {"type": "array"},
        "deleted_keys": {"type": "array"},
        "source": {"type": "string"},
        "target": {"type": "string"},
    },
    "required": ["bucket_name", "source", "target"],
}

from google.cloud import storage
import json
from jsonschema import validate, ValidationError

# Define the JSON Schema in-code
CITI_BIKE_SCHEMA = {
    "type": "object",
    "properties": {
        "network": {
            "type": "object",
            "properties": {
                "company": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                },
                "href": {"type": "string", "format": "uri"},
                "id": {"type": "string", "pattern": "^citi-bike-nyc$"},
                "location": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string", "pattern": "^New York(,? ?NY)?$"},
                        "country": {"type": "string", "pattern": "^US$"},
                        "latitude": {
                            "type": "number",
                            "minimum": 40.4,
                            "maximum": 40.9,
                        },
                        "longitude": {
                            "type": "number",
                            "minimum": -74.3,
                            "maximum": -73.7,
                        },
                    },
                    "required": ["city", "country", "latitude", "longitude"],
                },
                "name": {"type": "string", "pattern": "Citi Bike"},
                "stations": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "empty_slots": {"type": "number", "minimum": 0},
                            "extra": {
                                "type": "object",
                                "properties": {
                                    "ebikes": {"type": "number", "minimum": 0},
                                    "has_ebikes": {"type": "boolean"},
                                    "last_updated": {
                                        "type": "number",
                                        "minimum": 1_000_000_000,
                                    },
                                    "uid": {
                                        "anyOf": [
                                            {"type": "number", "minimum": 1},
                                            {
                                                "type": "string",
                                                "pattern": "^[a-f0-9\\-]{36}$",
                                            },
                                            {"type": "string", "pattern": "^[0-9]+$"},
                                        ]
                                    },
                                },
                                "required": [
                                    "ebikes",
                                    "has_ebikes",
                                    "last_updated",
                                    "uid",
                                ],
                            },
                            "free_bikes": {"type": "number", "minimum": 0},
                            "id": {"type": "string", "minLength": 1},
                            "latitude": {
                                "type": "number",
                                "minimum": 40.4,
                                "maximum": 40.9,
                            },
                            "longitude": {
                                "type": "number",
                                "minimum": -74.3,
                                "maximum": -73.7,
                            },
                            "name": {"type": "string", "minLength": 1},
                            "timestamp": {"type": "string", "format": "date-time"},
                        },
                        "required": [
                            "empty_slots",
                            "free_bikes",
                            "id",
                            "latitude",
                            "longitude",
                            "name",
                        ],
                    },
                },
            },
            "required": ["company", "href", "id", "location", "name", "stations"],
        }
    },
    "required": ["network"],
}


def validate_latest_file(**kwargs):

    bucket_name = kwargs.get("params", {}).get("bronze_bucket", "bronze")

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Find latest JSON blob
    blobs = list(bucket.list_blobs(prefix="raw/"))
    json_blobs = [blob for blob in blobs if blob.name.endswith(".json")]
    if not json_blobs:
        raise Exception("No JSON files found in the bucket.")

    latest_blob = max(json_blobs, key=lambda b: b.updated)
    print(f"Validating: {latest_blob.name}")

    # Validate against schema
    content = latest_blob.download_as_text()
    data = json.loads(content)
    try:
        validate(instance=data, schema=CITI_BIKE_SCHEMA)
        print("JSON is valid.")
    except ValidationError as e:
        print("Validationerror:")
        print(e)
        raise

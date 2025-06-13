import requests
import json
from datetime import datetime
from google.cloud import storage
from jsonschema import validate, ValidationError

# Citi Bike JSON Schema
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


def fetch_and_upload(**kwargs):

    API_URL = "https://api.citybik.es/v2/networks/citi-bike-nyc"

    try:
        # Get bucket name from Airflow params
        params = kwargs.get("params", {})
        bucket_name = params.get("bronze_bucket")

        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Validate JSON against schema
        try:
            validate(instance=data, schema=CITI_BIKE_SCHEMA)
            print("JSON is valid.")
        except ValidationError as ve:
            print("JSON schema validation failed.")
            print(str(ve))
            raise ValueError("JSON schema validation failed")

        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"raw/station_status_{timestamp}.json"

        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data))

        print(f"Successfully uploaded {filename} to {bucket_name}")
        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

"""NYC Taxi REST API pipeline using dlt."""

import dlt
from dlt.sources.rest_api import rest_api_resources


def taxi_pipeline():
    """Define dlt resources from the NYC Taxi REST API."""
    return rest_api_resources({
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "ny_taxi",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {
                        "page": 1,
                    },
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "maximum_page": 11,
                        "stop_after_empty_page": True,
                        "total_path": None
                    },
                },
            },
        ],
    })


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi",
    )

    load_info = pipeline.run(taxi_pipeline())
    print(load_info)

import sys
import os
import time
from typing import List

# Allow imports from the project root (db.py, etc.)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from db import get_connection
from ramapi import get_endpoint
from models import ApiParameters, CharacterSchema

from schema import bootstrap_schema
from api import fetch_all_products
from loader import land_all_products

END_POINT = "character"


def get_all_paginated_results(endpoint: str, pages: int, params: ApiParameters) -> List[CharacterSchema]:
    results = []
    for page in range(1, pages + 1):
        params.page = page
        print(f"Calling Page {page}")
        response = get_endpoint(endpoint, params)
        results.extend(response.results)
        time.sleep(0.3)  # brief pause to avoid rate limiting
    return results

def run_extract() -> None:
    with  get_connection() as conn:

        print("Successfully connected to PostgreSQL!")


        bootstrap_schema(conn)

        pages = fetch_all_products()

        land_all_products(conn , pages)

if __name__ == "__main__":
    # with get_connection() as conn:
    #     print("Successfully connected to PostgreSQL!")

    # params = ApiParameters()
    # response = get_endpoint(END_POINT, params)
    # results = get_all_paginated_results(END_POINT, response.info.pages, params)
    # print(f"Total records: {len(results)}")

    run_extract()

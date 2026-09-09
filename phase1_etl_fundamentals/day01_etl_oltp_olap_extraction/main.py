import time
from typing import List

from ramapi import get_endpoint
from models import ApiParameters, CharacterSchema

END_POINT = "character"


def get_all_paginated_results(endpoint: str , pages: int , params: ApiParameters) -> List[CharacterSchema]:

    results = []
    for page in range(1, pages + 1):
        params.page = page
        print(f"Calling Page {page}")
        response = get_endpoint(endpoint,params)
        results.extend(response.results)
        time.sleep(0.3)  # brief pause to avoid rate limiting

    return results



if __name__ == "__main__":
    params = ApiParameters()
    response = get_endpoint(END_POINT , params)
    results = get_all_paginated_results(END_POINT , response.info.pages , params)
    print(f"Total records: {len(results)}")
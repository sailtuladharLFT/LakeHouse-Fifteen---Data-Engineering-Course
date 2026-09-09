import time
import requests
from dataclasses import asdict

from models import ApiResponse, ApiParameters

BASE_URL = "https://rickandmortyapi.com/api"

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds to wait after a 429


def get_endpoint(endpoint: str , params: ApiParameters) -> ApiResponse:

    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(url = f"{BASE_URL}/{endpoint}" , params = asdict(params))

        if response.status_code == 429:
            print(f"Rate limited (429). Waiting {RETRY_DELAY}s before retry {attempt}/{MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)
            continue

        response.raise_for_status()
        return ApiResponse(**response.json())

    response.raise_for_status()  # raise if all retries exhausted



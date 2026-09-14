# Fetch paginated data from the DummyJSON Posts API.
# Mirrors the structure of day01's api.py so the pattern stays consistent.

import requests
import time
from dataclasses import dataclass
from typing import Optional

BASE_URL = "https://dummyjson.com/posts"


@dataclass
class PostsPageParams:
    skip: Optional[int] = 0
    limit: Optional[int] = 30   # fetch 30 posts per request


def fetch_page(params: PostsPageParams) -> dict:
    """Fetch a single paginated page from the /posts endpoint."""
    response = requests.get(BASE_URL, params={"skip": params.skip, "limit": params.limit})
    response.raise_for_status()
    return response.json()


def fetch_all_posts(params: PostsPageParams = PostsPageParams()) -> list[dict]:
    """
    Walk through every page of the /posts endpoint and return all raw pages.
    A probe request reads 'total' first so we know exactly how many iterations to run.
    Returns a list of raw page dicts — each contains a 'posts' key with the records.
    """
    # Probe the first page to learn the total record count
    first_page = fetch_page(PostsPageParams(skip=0, limit=1))
    total = first_page["total"]

    pages = []
    skip  = 0

    while skip < total:
        print(f"Fetching page {skip // params.limit + 1} of {total // params.limit + 1}")
        page = fetch_page(PostsPageParams(skip=skip, limit=params.limit))
        pages.append(page)
        skip += params.limit
        time.sleep(0.2)   # be polite to the API — avoid rate limiting

    return pages

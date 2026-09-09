from models import ProductsPageParams
import requests
import time

BASE_URL = "https://dummyjson.com/products"



def fetch_page(params: ProductsPageParams ) -> dict:
    response = requests.get(BASE_URL , params={"skip": params.skip, "limit": params.limit})
    response.raise_for_status()
    return response.json() 


def fetch_all_products(params: ProductsPageParams = ProductsPageParams()) -> list[dict]:
    first_page_res = fetch_page(ProductsPageParams(skip=0, limit=1))
    total = first_page_res["total"]

    pages = []
    skip = 0

    while skip < total:
        print(f"Fetching Page {skip // params.limit + 1} out of {total // params.limit + 1}")
        page = fetch_page(ProductsPageParams(skip=skip, limit=params.limit))
        pages.append(page)
        skip += params.limit
        time.sleep(0.2)

    return pages

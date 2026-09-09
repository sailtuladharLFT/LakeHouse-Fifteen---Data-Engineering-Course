# Load Data from API and write them into database

import json

def land_product(conn , raw_page: dict) -> None:
    
    for product in raw_page["products"]:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.stg_products
                (id, raw_payload)
                VALUES(%s , %s)
                ON CONFLICT (id) DO NOTHING
                """ , (
                    product["id"],
                    json.dumps(product)
                )
            )
    
    conn.commit()


def land_all_products(conn , pages: list[dict]):

    for page in pages:
        land_product(conn , page)
    
    print(f"{len(pages)} sent to DB")
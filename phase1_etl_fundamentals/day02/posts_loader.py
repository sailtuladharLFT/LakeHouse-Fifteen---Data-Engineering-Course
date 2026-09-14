# Load raw post data from the API into staging.stg_posts.
# Mirrors the structure of day01's loader.py so the pattern stays consistent.

import json


def land_post(conn, raw_page: dict) -> None:
    """
    Insert all posts from a single API page into staging.stg_posts.
    ON CONFLICT (id) DO NOTHING keeps this idempotent — re-running the DAG
    on the same day will not create duplicate staging rows.
    """
    for post in raw_page["posts"]:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.stg_posts
                (id, raw_payload)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    post["id"],
                    json.dumps(post),   # store the full post as JSONB
                )
            )

    conn.commit()


def land_all_posts(conn, pages: list[dict]) -> None:
    """Iterate over all fetched pages and land each one into staging."""
    for page in pages:
        land_post(conn, page)

    print(f"{len(pages)} page(s) landed into staging.stg_posts")

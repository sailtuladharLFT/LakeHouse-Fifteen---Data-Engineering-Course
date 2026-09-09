
def create_products_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.stg_products(
            id            SERIAL PRIMARY KEY,
            raw_payload   JSONB  NOT NULL,
            extracted_at  TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

def bootstrap_schema(conn) -> None:

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    
    create_products_table(conn)
    conn.commit()
    print("Schema for Products Table has been created")
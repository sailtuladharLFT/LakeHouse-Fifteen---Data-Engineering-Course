
def create_raw_products_table(conn):
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
def create_dim_category_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_category(
            category_key  SERIAL PRIMARY KEY,
            category_name TEXT UNIQUE NOT NULL,
            created_at    TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )


def create_dim_date_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_date(
            date_key  INT  PRIMARY KEY,
            full_date DATE NOT NULL,
            year      INT  NOT NULL,
            month     INT  NOT NULL,
            day       INT  NOT NULL,
            quarter   INT  NOT NULL
            );
            """
        )


def create_dim_product_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_product(
            product_sk  SERIAL  PRIMARY KEY,
            product_id  INT     NOT NULL,
            title       TEXT,
            brand       TEXT,
            description TEXT,
            price       NUMERIC(10,2),
            rating      NUMERIC(3,2),
            is_current  BOOLEAN NOT NULL DEFAULT TRUE,
            valid_from  DATE    NOT NULL,
            valid_to    DATE
            );
            """
        )
        # Partial unique index — only one current row allowed per product_id
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS udx_dim_product_current
            ON warehouse.dim_product(product_id)
            WHERE is_current = TRUE;
            """
        )


def create_fact_product_snapshot_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.fact_product_snapshot(
            snapshot_id  SERIAL      PRIMARY KEY,
            product_sk   INT         NOT NULL REFERENCES warehouse.dim_product(product_sk),
            category_key INT         NOT NULL REFERENCES warehouse.dim_category(category_key),
            date_key     INT         NOT NULL REFERENCES warehouse.dim_date(date_key),
            list_price   NUMERIC(10,2),
            discount_pct NUMERIC(5,2),
            stock        INT
            );
            """
        )


def bootstrap_schema(conn) -> None:

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")

    # Order matters — dims before fact (fact has FKs pointing to dims)
    create_raw_products_table(conn)
    create_dim_date_table(conn)
    create_dim_category_table(conn)
    create_dim_product_table(conn)
    create_fact_product_snapshot_table(conn)

    conn.commit()
    print("Bootstrap complete: staging + warehouse schemas and all tables created")

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


# ──────────────────────────────────────────────────────────────────────────────
# Day 03 — Posts pipeline tables
# ──────────────────────────────────────────────────────────────────────────────

def create_stg_posts_table(conn):
    """Staging table: raw JSONB payload per post, one row per API record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.stg_posts(
            id            INT PRIMARY KEY,
            raw_payload   JSONB NOT NULL,
            extracted_at  TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )


def create_dim_tag_table(conn):
    """Type 1 dimension: one row per unique tag name seen across all posts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_tag(
            tag_key   SERIAL PRIMARY KEY,
            tag_name  TEXT UNIQUE NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )


def create_dim_post_table(conn):
    """
    SCD Type 2 dimension: tracks changes to a post's title and body over time.
    When either field changes, the old row is expired (is_current → FALSE,
    valid_to set) and a new current row is inserted.
    The partial unique index ensures only one current row exists per post_id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_post(
            post_sk     SERIAL  PRIMARY KEY,
            post_id     INT     NOT NULL,
            title       TEXT,
            body        TEXT,
            user_id     INT,
            is_current  BOOLEAN NOT NULL DEFAULT TRUE,
            valid_from  DATE    NOT NULL,
            valid_to    DATE
            );
            """
        )
        # Partial unique index — only one current row allowed per post_id
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS udx_dim_post_current
            ON warehouse.dim_post(post_id)
            WHERE is_current = TRUE;
            """
        )


def create_bridge_post_tag_table(conn):
    """
    Bridge table: maps each dim_post surrogate key to one or more dim_tag keys.
    Needed because a post can have multiple tags (many-to-many relationship).
    The UNIQUE constraint on (post_sk, tag_key) makes re-runs idempotent.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.bridge_post_tag(
            bridge_id  SERIAL PRIMARY KEY,
            post_sk    INT    NOT NULL REFERENCES warehouse.dim_post(post_sk),
            tag_key    INT    NOT NULL REFERENCES warehouse.dim_tag(tag_key),
            UNIQUE(post_sk, tag_key)
            );
            """
        )


def create_fact_post_metrics_table(conn):
    """
    Fact table: one row per post per day capturing engagement metrics.
    user_id is denormalized here for easy author-level filtering without
    an extra join back through dim_post.
    Tag analysis goes through bridge_post_tag → dim_tag.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.fact_post_metrics(
            metrics_id  SERIAL  PRIMARY KEY,
            post_sk     INT     NOT NULL REFERENCES warehouse.dim_post(post_sk),
            date_key    INT     NOT NULL REFERENCES warehouse.dim_date(date_key),
            user_id     INT,
            likes       INT,
            dislikes    INT,
            views       INT
            );
            """
        )


# ──────────────────────────────────────────────────────────────────────────────
# Day 04 — Comments pipeline tables
# ──────────────────────────────────────────────────────────────────────────────

def create_stg_comments_table(conn):
    """Staging table: raw JSONB payload per comment, one row per API record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.stg_comments(
            id            INT PRIMARY KEY,
            raw_payload   JSONB NOT NULL,
            extracted_at  TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )


def create_dim_user_table(conn):
    """
    SCD Type 2 dimension: tracks changes to a user's username and full_name.
    When either field changes, the old row is expired (is_current → FALSE,
    valid_to set) and a new current row is inserted.
    The partial unique index ensures only one current row exists per user_id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.dim_user(
            user_sk     SERIAL  PRIMARY KEY,
            user_id     INT     NOT NULL,
            username    TEXT,
            full_name   TEXT,
            is_current  BOOLEAN NOT NULL DEFAULT TRUE,
            valid_from  DATE    NOT NULL,
            valid_to    DATE
            );
            """
        )
        # Partial unique index — only one current row allowed per user_id
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS udx_dim_user_current
            ON warehouse.dim_user(user_id)
            WHERE is_current = TRUE;
            """
        )


def create_fact_comment_metrics_table(conn):
    """
    Fact table: one row per comment per day capturing engagement metrics.
    post_id is denormalized here — the comments API gives us only a postId
    reference, not enough post content to justify a separate dim_post join.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS warehouse.fact_comment_metrics(
            metrics_id  SERIAL  PRIMARY KEY,
            comment_id  INT     NOT NULL,
            user_sk     INT     NOT NULL REFERENCES warehouse.dim_user(user_sk),
            post_id     INT     NOT NULL,
            date_key    INT     NOT NULL REFERENCES warehouse.dim_date(date_key),
            likes       INT
            );
            """
        )


def bootstrap_schema(conn) -> None:

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")

    # ── Day 02: Products pipeline ─────────────────────────────────────────────
    # Order matters — dims before fact (fact has FKs pointing to dims)
    create_raw_products_table(conn)
    create_dim_date_table(conn)
    create_dim_category_table(conn)
    create_dim_product_table(conn)
    create_fact_product_snapshot_table(conn)

    # ── Day 03: Posts pipeline ────────────────────────────────────────────────
    # stg_posts first, then dims, then bridge (needs dim FKs), then fact
    create_stg_posts_table(conn)
    create_dim_tag_table(conn)
    create_dim_post_table(conn)
    create_bridge_post_tag_table(conn)       # bridge after both dims it references
    create_fact_post_metrics_table(conn)     # fact last — references dim_post + dim_date

    # ── Day 04: Comments pipeline ─────────────────────────────────────────────
    # stg_comments first, then dim_user (SCD2), then fact (references both dim_user + dim_date)
    create_stg_comments_table(conn)
    create_dim_user_table(conn)
    create_fact_comment_metrics_table(conn)  # fact last — references dim_user + dim_date

    conn.commit()
    print("Bootstrap complete: staging + warehouse schemas and all tables created")
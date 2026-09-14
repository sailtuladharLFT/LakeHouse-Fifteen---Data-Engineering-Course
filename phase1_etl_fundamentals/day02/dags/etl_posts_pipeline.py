"""
Day 02 — Posts Pipeline
=======================
Second pipeline living alongside the products pipeline in day02.
Orchestrates the full flow for the DummyJSON Posts API:
  bootstrap → extract → dims (parallel) → bridge → quality gate → fact

DAG task graph:
    bootstrap_warehouse
            |
    extract_and_stage
            |
   ┌────────┼────────┐
   │        │        │
dim_date  dim_tag  dim_post_scd2
            │        │
            └───┬────┘
                │
        bridge_post_tag          ← waits for BOTH dim_tag + dim_post
                │
   ┌────────────┘
   │
dim_date ───────┬──── bridge_post_tag
                │
        quality_check_gate
                │
        load_fact_post_metrics
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

# ── Path helpers ──────────────────────────────────────────────────────────────
# Resolve directories so Python can find shared modules (db.py, schema.py,
# day01 models, and day02's own transform/runner.py) regardless of where
# Airflow launches the DAG from.
DAG_DIR       = Path(__file__).resolve().parent          # …/day02/dags/
DAY02_DIR     = DAG_DIR.parent                           # …/day02/
REPO_ROOT     = DAY02_DIR.parent.parent                  # repo root (has db.py)
DAY01_DIR     = REPO_ROOT / "phase1_etl_fundamentals" / "day01_etl_oltp_olap_extraction"
SQL_DIR       = DAY02_DIR / "sql"
TRANSFORM_DIR = DAY02_DIR / "transform"

for p in [str(REPO_ROOT), str(DAY01_DIR), str(DAY02_DIR), str(TRANSFORM_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="etl_posts_pipeline",
    schedule="@daily",          # runs once per day automatically
    start_date=datetime(2026, 9, 14),
    catchup=False,              # don't back-fill missed runs
    tags=["day02", "posts", "star-schema", "warehouse"],
)
def etl_posts_pipeline():

    # ── Task 1: Bootstrap — create warehouse schema + all tables if missing ───
    @task
    def bootstrap_warehouse():
        """
        Runs CREATE SCHEMA / CREATE TABLE IF NOT EXISTS for the whole warehouse.
        Covers both the products and posts pipeline tables — safe to re-run
        because every statement uses IF NOT EXISTS.
        """
        from db import get_connection
        from schema import bootstrap_schema

        with get_connection() as conn:
            bootstrap_schema(conn)
            print("Bootstrap complete")

    # ── Task 2: Extract from Posts API and land into staging.stg_posts ────────
    @task
    def extract_and_stage():
        """
        Fetches all pages from https://dummyjson.com/posts using skip/limit
        pagination, then inserts each post as raw JSONB into staging.stg_posts.
        ON CONFLICT DO NOTHING keeps this idempotent across re-runs.
        """
        from db import get_connection
        from posts_api import fetch_all_posts
        from posts_loader import land_all_posts

        pages = fetch_all_posts()

        with get_connection() as conn:
            land_all_posts(conn, pages)
            print(f"Staged {len(pages)} page(s) into staging.stg_posts")

    # ── Task 3a: Populate dim_date (static date range 2024–2030) ─────────────
    @task
    def load_dim_date():
        """
        Populates warehouse.dim_date using generate_series().
        Shared with the products pipeline — ON CONFLICT DO NOTHING is safe.
        Re-uses the same 02_load_dim_date.sql file used by etl_star_schema_pipeline.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "02_load_dim_date.sql"))

    # ── Task 3b: Populate dim_tag (Type 1 upsert) ────────────────────────────
    @task
    def load_dim_tag():
        """
        Explodes the tags JSONB array for every staged post and upserts each
        unique tag name into warehouse.dim_tag.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "07_load_dim_tag.sql"))

    # ── Task 3c: Apply Type 2 SCD logic to dim_post ───────────────────────────
    @task
    def load_dim_post_scd2():
        """
        Runs three SQL statements in order:
          A) Expire rows where title or body changed (is_current → FALSE)
          B) Insert new current row for every expired post
          C) Insert brand-new posts that never appeared before
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "08_load_dim_post_scd2.sql"))

    # ── Task 4: Populate bridge_post_tag ─────────────────────────────────────
    @task
    def load_bridge_post_tag():
        """
        Maps each current post surrogate key (post_sk) to all of its tag keys
        in warehouse.bridge_post_tag.
        Must run AFTER both dim_tag and dim_post are fully loaded — the bridge
        table holds FK references into both.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "09_load_bridge_post_tag.sql"))

    # ── Task 5: Quality gate — fails DAG if any check returns rows ───────────
    @task
    def quality_check_gate():
        """
        Runs 4 SQL assertions from 11_quality_checks_posts.sql.
        Each check returns 0 rows if healthy.
        runner.run_quality_checks() raises RuntimeError on failure,
        which marks this task FAILED and prevents load_fact_post_metrics from running.
        """
        from db import get_connection
        from runner import run_quality_checks

        with get_connection() as conn:
            run_quality_checks(conn, str(SQL_DIR / "11_quality_checks_posts.sql"))

    # ── Task 6: Load fact table — only runs if gate passes ───────────────────
    @task
    def load_fact_post_metrics():
        """
        Joins staging → dim_post (current rows) → dim_date (today's date key)
        to produce one fact row per post capturing likes, dislikes, and views.
        Runs only if quality_check_gate succeeds.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "10_load_fact_post_metrics.sql"))
            print("Fact table load complete")

    # ── Wire up the task dependencies ─────────────────────────────────────────
    bootstrapped = bootstrap_warehouse()
    staged       = extract_and_stage()
    dim_date     = load_dim_date()
    dim_tag      = load_dim_tag()
    dim_post     = load_dim_post_scd2()
    bridge       = load_bridge_post_tag()
    gate         = quality_check_gate()
    fact         = load_fact_post_metrics()

    # bootstrap must finish before extraction starts
    bootstrapped >> staged

    # all three dim loads run in parallel after staging completes
    staged >> [dim_date, dim_tag, dim_post]

    # bridge needs BOTH dim_tag and dim_post — waits for both to finish
    [dim_tag, dim_post] >> bridge

    # quality gate waits for dim_date AND bridge (which already waited for both dims)
    [dim_date, bridge] >> gate

    # fact load only runs if the gate passes
    gate >> fact


# Airflow discovers the DAG by calling the decorated function
etl_posts_pipeline()

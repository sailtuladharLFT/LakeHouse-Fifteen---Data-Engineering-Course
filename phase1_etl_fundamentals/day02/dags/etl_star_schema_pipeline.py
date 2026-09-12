"""
Day 02 — Star Schema ETL Pipeline
==================================
Orchestrates the full flow:
  extract → stage → dims (parallel) → quality gate → fact

DAG task graph:
    bootstrap_warehouse
            |
    extract_and_stage
            |
   ┌────────┼────────────────┐
   │        │                │
dim_date  dim_category  dim_product_scd2
   │        │                │
   └────────┴────────────────┘
            |
    quality_check_gate
            |
    load_fact_snapshot
"""

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

# ── path helpers ──────────────────────────────────────────────────────────────
# Resolve the repo root (two levels above day02/dags/) so we can import
# shared modules (db.py, day01 loader/api/schema/models) regardless of where
# Airflow launches the DAG from.
DAG_DIR      = Path(__file__).resolve().parent          # …/day02/dags/
DAY02_DIR    = DAG_DIR.parent                           # …/day02/
REPO_ROOT    = DAY02_DIR.parent.parent                  # repo root (has db.py)
DAY01_DIR    = REPO_ROOT / "phase1_etl_fundamentals" / "day01_etl_oltp_olap_extraction"
SQL_DIR      = DAY02_DIR / "sql"
TRANSFORM_DIR= DAY02_DIR / "transform"

# Add paths so Python can find the shared modules
for p in [str(REPO_ROOT), str(DAY01_DIR), str(TRANSFORM_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ── DAG definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="etl_star_schema_pipeline",
    schedule="@daily",          # runs once per day automatically
    start_date=datetime(2026, 9, 12),
    catchup=False,              # don't back-fill missed runs
    tags=["day02", "star-schema", "warehouse"],
)
def etl_star_schema_pipeline():

    # ── Task 1: Bootstrap — create warehouse schema + all tables if missing ──
    @task
    def bootstrap_warehouse():
        """
        Runs CREATE SCHEMA / CREATE TABLE IF NOT EXISTS for the whole warehouse.
        Safe to re-run every DAG execution — all statements are idempotent.
        """
        from db import get_connection
        from schema import bootstrap_schema

        with get_connection() as conn:
            bootstrap_schema(conn)
            print("Bootstrap complete")

    # ── Task 2: Extract from API and land into staging.stg_products ──────────
    @task
    def extract_and_stage():
        """
        Re-uses Day 01's fetch_all_products() + land_all_products().
        Inserts raw JSONB into staging.stg_products.
        ON CONFLICT DO NOTHING keeps this idempotent.
        """
        from db import get_connection
        from api import fetch_all_products
        from loader import land_all_products

        pages = fetch_all_products()

        with get_connection() as conn:
            land_all_products(conn, pages)
            print(f"Staged {len(pages)} page(s) into staging.stg_products")

    # ── Task 3a: Populate dim_date (static date range 2024–2030) ─────────────
    @task
    def load_dim_date():
        """
        Populates warehouse.dim_date using generate_series().
        ON CONFLICT DO NOTHING — safe to re-run every day.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "02_load_dim_date.sql"))

    # ── Task 3b: Populate dim_category (Type 1 upsert) ───────────────────────
    @task
    def load_dim_category():
        """
        Reads distinct categories from staging JSONB and upserts into
        warehouse.dim_category. New categories are added; existing ones skipped.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "03_load_dim_category.sql"))

    # ── Task 3c: Apply Type 2 SCD logic to dim_product ───────────────────────
    @task
    def load_dim_product_scd2():
        """
        Runs three SQL statements in order:
          A) Expire rows where price or rating changed (is_current → FALSE)
          B) Insert new current row for every expired product
          C) Insert brand-new products that never appeared before
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "04_load_dim_product_scd2.sql"))

    # ── Task 4: Quality gate — fails DAG if any check returns rows ───────────
    @task
    def quality_check_gate():
        """
        Runs 4 SQL assertions from 06_quality_checks.sql.
        Each check returns 0 rows if healthy.
        runner.run_quality_checks() raises RuntimeError on the first failure,
        which marks this task FAILED and prevents load_fact_snapshot from running.
        """
        from db import get_connection
        from runner import run_quality_checks

        with get_connection() as conn:
            run_quality_checks(conn, str(SQL_DIR / "06_quality_checks.sql"))

    # ── Task 5: Load fact table — only runs if gate passes ───────────────────
    @task
    def load_fact_snapshot():
        """
        Joins staging + three current dim rows to produce one fact row
        per product for today's date_key.
        Runs only if quality_check_gate succeeds.
        """
        from db import get_connection
        from runner import run_sql_file

        with get_connection() as conn:
            run_sql_file(conn, str(SQL_DIR / "05_load_fact_snapshot.sql"))
            print("Fact table load complete")

    # ── Wire up the task dependencies ────────────────────────────────────────
    bootstrapped  = bootstrap_warehouse()
    staged        = extract_and_stage()
    dim_date      = load_dim_date()
    dim_category  = load_dim_category()
    dim_product   = load_dim_product_scd2()
    gate          = quality_check_gate()
    fact          = load_fact_snapshot()

    # bootstrap must finish before extraction starts
    bootstrapped >> staged

    # all three dim loads can run in parallel after staging completes
    staged >> [dim_date, dim_category, dim_product]

    # quality gate waits for ALL three dims to finish
    [dim_date, dim_category, dim_product] >> gate

    # fact load only runs if the gate passes
    gate >> fact


# Airflow discovers the DAG by calling the decorated function
etl_star_schema_pipeline()

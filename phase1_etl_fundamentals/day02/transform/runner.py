import os


def run_sql_file(conn, file_path: str) -> None:
    """
    Read a .sql file from disk and execute it as a single statement block.
    Commits on success. Used for all transform and load steps.
    """
    with open(file_path, "r") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    print(f"[runner] executed: {os.path.basename(file_path)}")


def run_quality_checks(conn, file_path: str) -> None:
    """
    Run a SQL file that contains multiple quality-check SELECT statements
    separated by semicolons.

    Each check must return 0 rows to pass.
    If any check returns rows, a RuntimeError is raised — the Airflow task
    will fail and block the downstream fact load.
    """
    with open(file_path, "r") as f:
        raw = f.read()

    # Split on semicolon, drop blank or comment-only chunks
    statements = [
        s.strip()
        for s in raw.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]

    failed_checks = []

    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
            rows = cur.fetchall()
            if rows:
                # rows exist = the check found a problem
                failed_checks.append({"statement": stmt[:80], "rows": rows})

    if failed_checks:
        details = "\n".join(
            f"  ❌ {c['statement']}...\n     Failing rows: {c['rows']}"
            for c in failed_checks
        )
        raise RuntimeError(f"Quality gate failed — {len(failed_checks)} check(s) returned rows:\n{details}")

    print(f"[runner] all quality checks passed: {os.path.basename(file_path)}")

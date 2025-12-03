import os
import json
import psycopg2
from psycopg2.extras import DictCursor
from fireworks import LLM

# ----------------------------------------------------------
# DB CONNECTION
# ----------------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="canvas_lms"
    )

# ----------------------------------------------------------
# SCHEMA EXTRACTION
# ----------------------------------------------------------
TARGET_TABLES = {
    "courses",
    "course_sections",
    "course_section_enrollments",
    "enrollments",
    "users",
    "learning_outcomes",
    "learning_outcome_results",
    "outcome_proficiencies",
    "outcome_result_rollups",
    "assignments",
    "submissions"
}

def extract_schema(conn):
    cur = conn.cursor(cursor_factory=DictCursor)

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public';
    """)

    tables = [row["table_name"] for row in cur.fetchall()]
    tables = [t for t in tables if t in TARGET_TABLES]

    schema = {}

    for tbl in tables:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s;
        """, (tbl,))
        cols = [{"name": r["column_name"], "type": r["data_type"]} for r in cur.fetchall()]

        cur.execute("""
            SELECT 
                kcu.column_name AS fk_column,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_name=%s AND tc.constraint_type='FOREIGN KEY';
        """, (tbl,))
        fks = [{
            "column": r["fk_column"],
            "ref_table": r["ref_table"],
            "ref_column": r["ref_column"]
        } for r in cur.fetchall()]

        schema[tbl] = {"columns": cols, "foreign_keys": fks}

    return schema
# ----------------------------------------------------------
# LLM SQL GENERATION
# ----------------------------------------------------------
def generate_sql_queries(llm, schema_md, num_queries=20):
    prompt = f"""
You are an expert SQL generator. Using the following PostgreSQL schema:

{schema_md}

Generate {num_queries} realistic, diverse SQL queries for analytics and reporting.

Rules:
- Use joins when appropriate.
- Use WHERE filters, GROUP BY, ORDER BY.
- Use aggregates like COUNT, AVG, SUM.
- Use realistic column values (e.g., timestamps, ids, statuses).
- Use subqueries, windows, and nested SELECTs occasionally.
- The queries must be valid PostgreSQL.

Return ONLY a JSON array of SQL strings.
"""

    resp = llm.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        temperature=0.6,
        response_format={"type": "json_object"}
    )

    return json.loads(resp.choices[0].message.content)


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():
    api_key = os.getenv("FIREWORKS_API_KEY")
    if not api_key:
        raise RuntimeError("FIREWORKS_API_KEY missing")

    llm = LLM(
        model="accounts/fireworks/models/llama-v3p1-8b-instruct",
        deployment_type="serverless",
        api_key=api_key,
    )

    conn = get_conn()
    schema = extract_schema(conn)

    # Pretty markdown version for LLM
    schema_md = json.dumps(schema, indent=2)

    output = generate_sql_queries(llm, schema_md, num_queries=50)

    # Save to JSONL for RLFT pipeline
    with open("generated_sql_queries.jsonl", "w") as f:
        for q in output["queries"]:
            f.write(json.dumps({"query": q}) + "\n")

    print("Generated SQL queries saved to generated_sql_queries.jsonl")


if __name__ == "__main__":
    main()

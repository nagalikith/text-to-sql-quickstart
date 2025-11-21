import os
import time
import json
import pathlib
from typing import List

import duckdb
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from fireworks import LLM


class SqlQueryBatch(BaseModel):
    queries: List[str] = Field(description="List of SQL queries")


def main() -> None:
    load_dotenv()
    root = pathlib.Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    synth_db = str(data_dir / "synthetic_openflights.db")
    out_path = data_dir / "generated_queries.json"
    api_key = os.getenv("FIREWORKS_API_KEY")
    if not api_key:
        raise RuntimeError("FIREWORKS_API_KEY is not set")

    llm = LLM(model="accounts/fireworks/models/llama-v3p1-8b-instruct", deployment_type="serverless", api_key=api_key)
    TOTAL = int(os.environ.get("TOTAL_QUERIES", "500"))
    BATCH = int(os.environ.get("QUERIES_PER_API_CALL", "30"))

    with duckdb.connect(synth_db, read_only=True) as con:
        schema_df = con.sql("DESCRIBE;").df()
    schema_md = schema_df.to_markdown(index=False)

    base_prompt = f"""
You are an expert SQL analyst. Generate diverse DuckDB-valid SQL queries across the schema.
Cover joins, groups, aggregates, and ensure deterministic ordering where applicable.
Avoid duplicate rows; group appropriately. Output only queries in JSON schema.

Schema:
{schema_md}
""".strip()

    all_q: List[str] = []
    while len(all_q) < TOTAL:
        existing = ""
        if all_q:
            existing = "Existing queries:\n" + "\n".join(f"{i + 1}. {q}" for i, q in enumerate(all_q[-100:]))
        prompt = base_prompt + "\n" + existing + f"\nGenerate {BATCH} new, unique queries as JSON."
        resp = llm.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "SqlQueryBatch", "schema": SqlQueryBatch.model_json_schema()},
            },
            temperature=0.8,
        )
        content = resp.choices[0].message.content
        if not content:
            time.sleep(1)
            continue
        try:
            new_q = json.loads(content).get("queries", [])
        except Exception:
            new_q = []
        all_q.extend(new_q)
        print(f"Got {len(new_q)} queries; total {len(all_q)}/{TOTAL}")
        time.sleep(1)

    # Dedup preserve order
    unique = list(dict.fromkeys(all_q))[:TOTAL]
    out_path.write_text(json.dumps({"queries": unique}, indent=2))
    print(f"Wrote {len(unique)} queries to {out_path}")


if __name__ == "__main__":
    main()

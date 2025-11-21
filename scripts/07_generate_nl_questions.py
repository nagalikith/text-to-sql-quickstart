import os
import json
import time
import random
import pathlib
from typing import List, Dict, Any

import jsonlines
import duckdb
from dotenv import load_dotenv
from fireworks import LLM


def main() -> None:
    load_dotenv()
    root = pathlib.Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    gt_path = data_dir / "ground_truth_results.jsonl"
    out_train = root / "datasets" / "final_rft_sql_train_data.jsonl"
    out_test = root / "datasets" / "final_rft_sql_test_data.jsonl"
    (root / "datasets").mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("FIREWORKS_API_KEY")
    if not api_key:
        raise RuntimeError("FIREWORKS_API_KEY is not set")
    llm = LLM(model="accounts/fireworks/models/llama-v3p1-8b-instruct", deployment_type="serverless", api_key=api_key)

    # Load schema for prompt (from synthetic DB)
    synth_db = str(data_dir / "synthetic_openflights.db")
    with duckdb.connect(synth_db, read_only=True) as con:
        schema_md = con.sql("DESCRIBE;").df().to_markdown(index=False)

    system_prompt = f"""
You are an expert SQL data analyst.
Write a single DuckDB SQL query to answer the user's question based on the schema.
Return only the SQL text, no explanations, and avoid duplicates via GROUP BY when needed.

Schema:
{schema_md}
""".strip()

    nl_template = """
Translate the SQL query into a natural language business question that would produce it.
Be precise and faithful to the SQL intent.

Schema:
{schema}

SQL:
{query}

Return only the question text.
""".strip()

    pairs: List[Dict[str, Any]] = []
    with jsonlines.open(gt_path) as reader:
        for obj in reader:
            pairs.append(obj)
    print(f"Loaded {len(pairs)} query-result pairs.")

    final_rows: List[Dict[str, Any]] = []
    for i, pair in enumerate(pairs):
        query = pair["query"]
        ground_truth = pair["result"]
        user_prompt = nl_template.format(schema=schema_md, query=query)
        resp = llm.chat.completions.create(messages=[{"role": "user", "content": user_prompt}], temperature=0.5)
        nl = (resp.choices[0].message.content or "").strip()
        if not nl:
            continue
        final_rows.append(
            {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": nl},
                    {"role": "assistant", "content": query},
                ],
                "ground_truth": ground_truth,
            }
        )
        time.sleep(0.3)

    print(f"Generated {len(final_rows)} total examples; filtering empties.")
    final_rows = [r for r in final_rows if r.get("ground_truth")]
    random.seed(42)
    random.shuffle(final_rows)
    split_idx = int(len(final_rows) * 0.8)
    train = final_rows[:split_idx]
    test = final_rows[split_idx:]
    with jsonlines.open(out_train, mode="w") as w:
        w.write_all(train)
    with jsonlines.open(out_test, mode="w") as w:
        w.write_all(test)
    print(f"Wrote train={len(train)} to {out_train}, test={len(test)} to {out_test}")


if __name__ == "__main__":
    main()

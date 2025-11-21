import json
import pathlib

import duckdb
import pandas as pd


def main() -> None:
    root = pathlib.Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    synth_db = str(data_dir / "synthetic_openflights.db")
    queries_path = data_dir / "generated_queries.json"
    out_path = data_dir / "ground_truth_results.jsonl"

    with open(queries_path, "r") as f:
        queries = json.load(f).get("queries", [])

    MAX_ROWS = 1000
    MAX_BYTES = 100_000
    kept = 0
    failed = 0
    oversized = 0
    with open(out_path, "w") as out_f:
        with duckdb.connect(synth_db, read_only=True) as con:
            for q in queries:
                try:
                    df = con.sql(q).df()
                    df = df.astype(object).where(pd.notna(df), None)
                    records = df.to_dict("records")
                    if len(records) > MAX_ROWS:
                        oversized += 1
                        continue
                    payload = json.dumps(records, ensure_ascii=False)
                    if len(payload.encode("utf-8")) > MAX_BYTES:
                        oversized += 1
                        continue
                    out_f.write(json.dumps({"query": q, "result": records}) + "\n")
                    kept += 1
                except Exception:
                    failed += 1
    print(f"Ground truth saved: {out_path} | kept={kept}, failed={failed}, oversized={oversized}")


if __name__ == "__main__":
    main()

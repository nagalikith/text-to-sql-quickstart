import os
import json
import math
from typing import Any, Dict, List
from pathlib import Path

import requests
from eval_protocol.models import EvaluateResult, EvaluationRow
from eval_protocol.pytest import evaluation_test
from eval_protocol.pytest.default_single_turn_rollout_process import SingleTurnRolloutProcessor


def _parse_duckdb_ascii(table: str) -> List[Dict[str, Any]]:
    lines = [ln for ln in table.strip().split("\n") if ln.strip() and not ln.startswith("+")]
    if len(lines) < 2:
        return []
    headers = [h.strip() for h in lines[0].split("|")[1:-1]]
    data_lines = lines[1:]
    # Drop accidental uppercase-headers row (rare)
    if data_lines:
        try:
            first_vals = [v.strip() for v in data_lines[0].split("|")[1:-1]]
            if len(first_vals) == len(headers) and all(v.isupper() for v in first_vals):
                data_lines = data_lines[1:]
        except Exception:
            pass
    out: List[Dict[str, Any]] = []
    for ln in data_lines:
        vals = [v.strip() for v in ln.split("|")[1:-1]]
        if len(vals) != len(headers):
            continue
        row: Dict[str, Any] = {}
        for k, v in zip(headers, vals):
            if v.upper() == "NULL" or v == "":
                row[k] = None
                continue
            try:
                if "." in v:
                    row[k] = float(v)
                else:
                    row[k] = int(v)
            except Exception:
                row[k] = v
        out.append(row)
    return out


def evaluate(messages: List[Dict[str, str]], ground_truth: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
    mcp_url = os.getenv("MCP_SERVER_URL")
    if not mcp_url:
        return {"score": 0, "is_score_valid": False, "reason": "MCP_SERVER_URL not set"}
    if not messages or "content" not in messages[-1]:
        return {"score": 0, "reason": "No assistant output"}
    sql_query = (messages[-1]["content"] or "").strip()
    if not sql_query:
        return {"score": 0, "reason": "Empty assistant output"}
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    payload = {
        "id": "eval-1",
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {"session": {"id": "stateless-eval"}, "name": "query", "arguments": {"query": sql_query}},
    }
    try:
        with requests.post(f"{mcp_url}/mcp/", headers=headers, json=payload, timeout=20, stream=True) as r:
            r.raise_for_status()
            resp = None
            for line in r.iter_lines():
                if line:
                    txt = line.decode("utf-8")
                    if txt.startswith("data:"):
                        js = txt[5:].strip()
                        if js:
                            resp = json.loads(js)
                            break
        if not resp:
            return {"score": 0, "reason": "No event-stream JSON found"}
        if "error" in resp:
            return {"score": 0, "reason": f"MCP error: {resp['error']}"}
        ascii_table = resp["result"]["content"][0]["text"]
        pred = _parse_duckdb_ascii(ascii_table)
    except Exception as e:
        return {"score": 0, "reason": f"MCP request failed: {e}"}

    if not isinstance(ground_truth, list):
        return {"score": 0, "is_score_valid": False, "reason": "ground_truth was not a list"}

    def norm(v: Any) -> str:
        if v is None:
            return "None"
        if isinstance(v, float) and not (math.isinf(v) or math.isnan(v)) and v == int(v):
            v = int(v)
        return str(v)

    try:
        gt_vals = sorted([sorted(map(norm, r.values())) for r in ground_truth])
        pr_vals = sorted([sorted(map(norm, r.values())) for r in pred])
        ok = gt_vals == pr_vals
        return {"score": 1 if ok else 0, "reason": "match" if ok else f"mismatch: gt={ground_truth} pred={pred}"}
    except Exception as e:
        return {"score": 0, "reason": f"compare error: {e}"}


def _coerce_messages_for_eval(row_messages: List[Any]) -> List[Dict[str, str]]:
    """
    Convert EvaluationRow.messages (pydantic Message models) to simple {role, content} dicts.
    """
    out: List[Dict[str, str]] = []
    for m in row_messages:
        try:
            role = getattr(m, "role", None) if not isinstance(m, dict) else m.get("role")
            content = getattr(m, "content", None) if not isinstance(m, dict) else m.get("content")
            if isinstance(content, list) and content and isinstance(content[0], dict):
                text = content[0].get("text", "")
            else:
                text = content if isinstance(content, str) else ""
            # remove thinking content
            if "</think>" in text:
                text = text.split("</think>")[1]
            out.append({"role": role or "", "content": text})
        except Exception:
            continue
    return out


def _load_eval_rows(max_rows: int = 5) -> List[EvaluationRow]:
    root = Path(__file__).resolve().parents[1]
    ds_path = root / "datasets" / "final_rft_sql_test_data.jsonl"
    rows: List[EvaluationRow] = []
    if not ds_path.exists():
        return rows
    with open(ds_path, "r") as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                break
            obj = json.loads(line)
            er = EvaluationRow(messages=obj.get("messages", []), ground_truth=obj.get("ground_truth"))
            rows.append(er)
    return rows


@evaluation_test(
    input_rows=[_load_eval_rows(max_rows=5)],
    completion_params=[
        {
            "temperature": 0.0,
            "model": "fireworks_ai/accounts/fireworks/models/qwen3-8b",
        }
    ],
    rollout_processor=SingleTurnRolloutProcessor(),
    passed_threshold=0.0,
    num_runs=1,
    mode="pointwise",
    max_dataset_rows=5,
)
def test_sql_rft_local(row: EvaluationRow) -> EvaluationRow:
    """
    Local evaluation test: uses SingleTurnRolloutProcessor to have the model produce SQL,
    then evaluates via MCP server against ground_truth.
    Run with: pytest evaluator/sql_rft_evaluator.py -vs
    Environment: export MCP_SERVER_URL=http://127.0.0.1:8080
    """
    if not row.messages or row.ground_truth is None:
        row.evaluation_result = EvaluateResult(
            score=0.0, reason="Missing messages or ground_truth", is_score_valid=False
        )
        return row

    # Ensure MCP server URL default for local dev if not set
    if not os.getenv("MCP_SERVER_URL"):
        os.environ["MCP_SERVER_URL"] = "http://127.0.0.1:8080"

    msgs = _coerce_messages_for_eval(row.messages)
    res = evaluate(messages=msgs, ground_truth=row.ground_truth if isinstance(row.ground_truth, list) else [])
    score = float(res.get("score", 0))
    reason = res.get("reason")
    is_valid = bool(res.get("is_score_valid", True))
    row.evaluation_result = EvaluateResult(score=score, reason=reason, is_score_valid=is_valid)
    return row

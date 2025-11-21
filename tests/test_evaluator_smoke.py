import os
import json
import types
import importlib.util
from pathlib import Path


def load_evaluator():
    root = Path(__file__).resolve().parents[1]
    path = root / "evaluator" / "sql_rft_evaluator.py"
    spec = importlib.util.spec_from_file_location("sql_rft_evaluator", path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def test_ascii_parser_basic():
    mod = load_evaluator()
    parser = getattr(mod, "_parse_duckdb_ascii")
    table = (
        "+---------+-------+\n"
        "| country | cnt   |\n"
        "+---------+-------+\n"
        "| US      | 2     |\n"
        "| FR      | 1     |\n"
        "+---------+-------+\n"
    )
    rows = parser(table)
    assert rows == [{"country": "US", "cnt": 2}, {"country": "FR", "cnt": 1}]


def test_evaluate_missing_env_returns_safe_error(monkeypatch):
    mod = load_evaluator()
    evaluate = getattr(mod, "evaluate")
    monkeypatch.delenv("MCP_SERVER_URL", raising=False)
    msgs = [{"role": "assistant", "content": "SELECT 1"}]
    res = evaluate(msgs, ground_truth=[{"1": 1}])
    assert res["score"] == 0
    assert "MCP_SERVER_URL" in res.get("reason", "") or res.get("is_score_valid") is False

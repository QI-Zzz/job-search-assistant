"""Shared helpers for the LLM steps (run in the LLM virtualenv)."""
import os
from concurrent.futures import ThreadPoolExecutor

import anthropic

MAX_WORKERS = 5


def get_client():
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def format_posting(title, company, location, description):
    return f"Title: {title}\nCompany: {company}\nLocation: {location}\n\n{description}"


def call_structured(client, output_format, **request):
    """One Messages API call validated against a Pydantic model.

    Never raises: returns a row with status 'ok' or 'failed', so one job failing
    does not stop the run.
    """
    row = {"status": "failed", "result": None, "raw_response": None,
           "error": None, "input_tokens": None, "output_tokens": None}
    try:
        response = client.messages.parse(output_format=output_format, **request)
        row["raw_response"] = response.model_dump(mode="json")
        usage = response.usage
        row["input_tokens"] = (usage.input_tokens + (usage.cache_read_input_tokens or 0)
                               + (usage.cache_creation_input_tokens or 0))
        row["output_tokens"] = usage.output_tokens
        if response.stop_reason != "end_turn" or response.parsed_output is None:
            row["error"] = f"stop_reason={response.stop_reason}, no valid output"
        else:
            row["status"] = "ok"
            row["result"] = response.parsed_output.model_dump(mode="json")
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
    return row


def run_parallel(fn, items):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        return list(pool.map(fn, items))


def report(label, rows):
    ok = sum(r["status"] == "ok" for r in rows)
    tokens_in = sum(r["input_tokens"] or 0 for r in rows)
    tokens_out = sum(r["output_tokens"] or 0 for r in rows)
    print(f"{label}: {len(rows)} jobs, {ok} ok, {len(rows) - ok} failed; "
          f"{tokens_in} input / {tokens_out} output tokens")
    for r in rows:
        if r["status"] != "ok":
            print(f"  failed {r['job_id']}: {r['error']}")
    if rows and ok == 0:
        raise RuntimeError(f"{label}: every call failed")

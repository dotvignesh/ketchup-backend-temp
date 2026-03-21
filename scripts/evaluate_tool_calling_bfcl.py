#!/usr/bin/env python3
"""Evaluate vLLM tool-calling accuracy on a 25-example BFCL sample.

Benchmark choice:
- Berkeley Function Calling Leaderboard (BFCL) from Hugging Face
- executable simple subset (`BFCL_v3_exec_simple.json`)

Why this subset:
- explicit ground-truth tool calls are included,
- single-turn examples keep the script detached from the repo's planner setup,
- 25 sampled examples make it a fast smoke test for tool-calling accuracy.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from typing import Any

import httpx

DEFAULT_BENCHMARK_URL = (
    "https://huggingface.co/datasets/gorilla-llm/"
    "Berkeley-Function-Calling-Leaderboard/raw/main/BFCL_v3_exec_simple.json"
)


def _stream_json_objects(text: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    items: list[dict[str, Any]] = []
    index = 0

    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        item, next_index = decoder.raw_decode(text, index)
        if not isinstance(item, dict):
            raise ValueError("Expected each benchmark entry to be a JSON object.")
        items.append(item)
        index = next_index

    return items


def _normalize_schema(node: Any) -> Any:
    if isinstance(node, dict):
        normalized: dict[str, Any] = {}
        for key, value in node.items():
            if key == "type" and isinstance(value, str):
                normalized[key] = {
                    "dict": "object",
                    "float": "number",
                    "bool": "boolean",
                }.get(value, value)
            else:
                normalized[key] = _normalize_schema(value)
        return normalized
    if isinstance(node, list):
        return [_normalize_schema(item) for item in node]
    return node


def _tool_spec(entry: dict[str, Any]) -> list[dict[str, Any]]:
    tools = []
    for function in entry["function"]:
        tool = _normalize_schema(function)
        tools.append({"type": "function", "function": tool})
    return tools


def _parse_ground_truth(call_text: str) -> tuple[str, dict[str, Any]]:
    expr = ast.parse(call_text, mode="eval").body
    if not isinstance(expr, ast.Call):
        raise ValueError(f"Unsupported ground truth call: {call_text}")

    def build_name(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return f"{build_name(node.value)}.{node.attr}"
        raise ValueError(f"Unsupported callable node in ground truth: {call_text}")

    name = build_name(expr.func)
    arguments = {keyword.arg: ast.literal_eval(keyword.value) for keyword in expr.keywords}
    return name, arguments


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_values_equal(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_values_equal(left[k], right[k]) for k in left)
    return left == right


def _extract_messages(entry: dict[str, Any]) -> list[dict[str, str]]:
    question = entry["question"]
    if not question or not isinstance(question, list):
        raise ValueError(f"Unexpected question shape for entry {entry.get('id')}")
    conversation = question[0]
    if not isinstance(conversation, list):
        raise ValueError(f"Unexpected conversation shape for entry {entry.get('id')}")
    return [{"role": turn["role"], "content": turn["content"]} for turn in conversation]


def _extract_tool_calls(response: dict[str, Any]) -> list[dict[str, Any]]:
    choices = response.get("choices") or []
    if not choices:
        return []
    message = choices[0].get("message") or {}
    return message.get("tool_calls") or []


def _extract_prediction(response: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    tool_calls = _extract_tool_calls(response)
    if not tool_calls:
        return None, None, None

    first_call = tool_calls[0]
    function = first_call.get("function") or {}
    name = function.get("name")
    raw_arguments = function.get("arguments")

    if not isinstance(raw_arguments, str):
        return name, None, None

    try:
        arguments = json.loads(raw_arguments)
    except json.JSONDecodeError:
        return name, None, raw_arguments
    return name, arguments, raw_arguments


def _normalize_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        normalized = normalized[: -len("/chat/completions")]
    return normalized


def _candidate_completion_urls(base_url: str) -> list[str]:
    normalized = _normalize_base_url(base_url)
    candidates = [f"{normalized}/chat/completions"]

    if normalized.endswith("/v1"):
        root = normalized[: -len("/v1")]
        if root:
            candidates.append(f"{root}/chat/completions")
    else:
        candidates.insert(0, f"{normalized}/v1/chat/completions")

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def _candidate_models_urls(base_url: str) -> list[str]:
    normalized = _normalize_base_url(base_url)
    candidates = [f"{normalized}/models"]
    if normalized.endswith("/v1"):
        root = normalized[: -len("/v1")]
        if root:
            candidates.append(f"{root}/models")
    else:
        candidates.insert(0, f"{normalized}/v1/models")

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def _describe_base_url(base_url: str) -> str:
    parts = urlsplit(base_url)
    path = parts.path or "/"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _ensure_endpoint_available(client: httpx.Client, base_url: str) -> str:
    model_urls = _candidate_models_urls(base_url)
    completion_urls = _candidate_completion_urls(base_url)

    for model_url, completion_url in zip(model_urls, completion_urls, strict=False):
        try:
            response = client.get(model_url, timeout=30.0)
        except httpx.HTTPError:
            continue
        if response.status_code == 200:
            return completion_url

    errors = []
    for model_url in model_urls:
        try:
            response = client.get(model_url, timeout=30.0)
            errors.append(f"{model_url} -> HTTP {response.status_code}")
        except httpx.HTTPError as exc:
            errors.append(f"{model_url} -> {exc.__class__.__name__}")

    raise RuntimeError(
        "Could not find an OpenAI-compatible models endpoint for the provided base URL "
        f"{_describe_base_url(base_url)}. Tried: {', '.join(errors)}. "
        "If you passed your backend API URL by mistake, point this script at the raw vLLM server instead, "
        "for example `http://localhost:8080/v1`."
    )


def _allocate_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _spawn_local_vllm_server(
    *,
    model_ref: str,
    served_model_name: str,
    host: str,
    port: int,
    gpu_memory_utilization: float,
    max_model_len: int,
    max_num_seqs: int,
    tool_call_parser: str,
    log_path: Path,
) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--host",
        host,
        "--port",
        str(port),
        "--model",
        model_ref,
        "--served-model-name",
        served_model_name,
        "--gpu-memory-utilization",
        str(gpu_memory_utilization),
        "--max-model-len",
        str(max_model_len),
        "--max-num-seqs",
        str(max_num_seqs),
        "--enable-auto-tool-choice",
        "--tool-call-parser",
        tool_call_parser,
    ]

    log_handle = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        command,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        env=os.environ.copy(),
    )


def _read_log_excerpt(log_path: Path, max_chars: int = 4000) -> str:
    if not log_path.exists():
        return "(no log file created)"
    text = log_path.read_text(encoding="utf-8", errors="replace")
    if not text.strip():
        return "(log file is empty)"
    return text[-max_chars:]


def _wait_for_server(
    base_url: str,
    timeout_seconds: float,
    process: subprocess.Popen[str] | None = None,
    log_path: Path | None = None,
) -> str:
    deadline = time.time() + timeout_seconds
    last_error: str | None = None

    with httpx.Client() as client:
        while time.time() < deadline:
            if process is not None:
                return_code = process.poll()
                if return_code is not None:
                    log_excerpt = _read_log_excerpt(log_path) if log_path is not None else "(no logs available)"
                    raise RuntimeError(
                        "Local vLLM server exited before becoming ready "
                        f"(exit code {return_code}). Startup log tail:\n{log_excerpt}"
                    )
            try:
                return _ensure_endpoint_available(client, base_url)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(2)

    if process is not None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    log_suffix = ""
    if log_path is not None:
        log_suffix = f"\nStartup log tail:\n{_read_log_excerpt(log_path)}"
    raise RuntimeError(
        "Timed out waiting for the local vLLM server to become ready. "
        f"Last error: {last_error}{log_suffix}"
    )


def _call_model(
    client: httpx.Client,
    *,
    completions_url: str,
    model: str,
    messages: list[dict[str, str]],
    tools: list[dict[str, Any]],
    temperature: float,
    max_tokens: int,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    response = client.post(completions_url, json=payload, timeout=120.0)
    if response.status_code == 400 and "tool_choice" in response.text:
        payload.pop("tool_choice", None)
        response = client.post(completions_url, json=payload, timeout=120.0)
    response.raise_for_status()
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-url",
        default=None,
        help="Existing OpenAI-compatible vLLM base URL. If omitted, the script launches a local vLLM server.",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Served model name reported to the OpenAI-compatible endpoint.",
    )
    parser.add_argument(
        "--model-ref",
        default=None,
        help="Model path or HF repo passed to local vLLM when spawning a server. Defaults to `--model`.",
    )
    parser.add_argument("--sample-size", type=int, default=25, help="Number of benchmark examples to evaluate.")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed for reproducible 25-example slices.")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--benchmark-url", default=DEFAULT_BENCHMARK_URL)
    parser.add_argument("--host", default="127.0.0.1", help="Host for the local vLLM server.")
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port for the local vLLM server. Use 0 to auto-select a free port.",
    )
    parser.add_argument("--startup-timeout", type=float, default=180.0, help="Seconds to wait for local vLLM readiness.")
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.7)
    parser.add_argument("--max-model-len", type=int, default=4096)
    parser.add_argument("--max-num-seqs", type=int, default=4)
    parser.add_argument("--tool-call-parser", default="hermes")
    parser.add_argument(
        "--output",
        default="data/reports/tool_calling_bfcl_25.json",
        help="Path for the evaluation summary JSON.",
    )
    args = parser.parse_args()

    local_process: subprocess.Popen[str] | None = None
    log_path: Path | None = None
    base_url = args.base_url
    if base_url is None:
        port = args.port or _allocate_free_port(args.host)
        base_url = f"http://{args.host}:{port}/v1"
        model_ref = args.model_ref or args.model
        log_path = Path(tempfile.gettempdir()) / f"evaluate_tool_calling_bfcl_vllm_{port}.log"
        print(f"Starting local vLLM server for model ref: {model_ref}")
        print(f"Using local port: {port}")
        print(f"vLLM startup log: {log_path}")
        local_process = _spawn_local_vllm_server(
            model_ref=model_ref,
            served_model_name=args.model,
            host=args.host,
            port=port,
            gpu_memory_utilization=args.gpu_memory_utilization,
            max_model_len=args.max_model_len,
            max_num_seqs=args.max_num_seqs,
            tool_call_parser=args.tool_call_parser,
            log_path=log_path,
        )
        try:
            completions_url = _wait_for_server(
                base_url,
                args.startup_timeout,
                process=local_process,
                log_path=log_path,
            )
        except Exception:
            if local_process.poll() is None:
                local_process.kill()
                local_process.wait(timeout=10)
            raise
    else:
        with httpx.Client() as client:
            completions_url = _ensure_endpoint_available(client, base_url)

    dataset_response = httpx.get(args.benchmark_url, timeout=120.0)
    dataset_response.raise_for_status()
    entries = _stream_json_objects(dataset_response.text)
    if args.sample_size > len(entries):
        raise ValueError(f"Requested {args.sample_size} examples but benchmark only has {len(entries)} rows.")

    rng = random.Random(args.seed)
    sampled_entries = rng.sample(entries, args.sample_size)

    results: list[dict[str, Any]] = []
    tool_name_hits = 0
    argument_hits = 0
    exact_hits = 0

    try:
        with httpx.Client() as client:
            print(f"Using completions endpoint: {completions_url}")
            for entry in sampled_entries:
                expected_name, expected_arguments = _parse_ground_truth(entry["ground_truth"][0])
                response = _call_model(
                    client,
                    completions_url=completions_url,
                    model=args.model,
                    messages=_extract_messages(entry),
                    tools=_tool_spec(entry),
                    temperature=args.temperature,
                    max_tokens=args.max_tokens,
                )

                predicted_name, predicted_arguments, raw_arguments = _extract_prediction(response)
                name_match = predicted_name == expected_name
                args_match = predicted_arguments is not None and _values_equal(predicted_arguments, expected_arguments)
                exact_match = name_match and args_match

                tool_name_hits += int(name_match)
                argument_hits += int(args_match)
                exact_hits += int(exact_match)

                results.append(
                    {
                        "id": entry["id"],
                        "question": _extract_messages(entry)[-1]["content"],
                        "expected_name": expected_name,
                        "expected_arguments": expected_arguments,
                        "predicted_name": predicted_name,
                        "predicted_arguments": predicted_arguments,
                        "raw_arguments": raw_arguments,
                        "tool_name_match": name_match,
                        "arguments_match": args_match,
                        "exact_match": exact_match,
                        "tool_call_count": len(_extract_tool_calls(response)),
                    }
                )
    finally:
        if local_process is not None:
            local_process.terminate()
            try:
                local_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                local_process.kill()
                local_process.wait(timeout=15)

    summary = {
        "benchmark": "gorilla-llm/Berkeley-Function-Calling-Leaderboard",
        "benchmark_subset": "BFCL_v3_exec_simple.json",
        "benchmark_url": args.benchmark_url,
        "sample_size": args.sample_size,
        "seed": args.seed,
        "model": args.model,
        "base_url": base_url,
        "tool_name_accuracy": tool_name_hits / args.sample_size,
        "arguments_accuracy": argument_hits / args.sample_size,
        "exact_match_accuracy": exact_hits / args.sample_size,
        "results": results,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Benchmark: {summary['benchmark']} / {summary['benchmark_subset']}")
    print(f"Model: {args.model}")
    print(f"Sample size: {args.sample_size}")
    print(f"Tool name accuracy: {summary['tool_name_accuracy']:.2%}")
    print(f"Arguments accuracy: {summary['arguments_accuracy']:.2%}")
    print(f"Exact match accuracy: {summary['exact_match_accuracy']:.2%}")
    print(f"Saved summary: {output_path}")

    failures = [item for item in results if not item["exact_match"]]
    if failures:
        print("\nFirst 5 failures:")
        for failure in failures[:5]:
            print(
                f"- {failure['id']}: expected {failure['expected_name']} "
                f"{_canonical_json(failure['expected_arguments'])}, got "
                f"{failure['predicted_name']} {_canonical_json(failure['predicted_arguments']) if failure['predicted_arguments'] is not None else failure['raw_arguments']}"
            )


if __name__ == "__main__":
    main()

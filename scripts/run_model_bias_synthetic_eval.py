#!/usr/bin/env python3
"""Run a synthetic slicing-based bias evaluation against a vLLM endpoint."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests import Response

PRICE_LEVELS = ["$", "$$", "$$$"]
CATEGORIES = [
    "pizza",
    "thai",
    "mexican",
    "sushi",
    "bbq",
    "salad",
    "ramen",
    "coffee",
    "museum",
    "bowling",
    "karaoke",
    "park",
    "hike",
]

SYSTEM_PROMPT = """You are Ketchup, a group hangout planner.

Return STRICT JSON only (no markdown, no extra text).
Pick exactly 3 venues from the provided list that best match constraints.
Respect budget and distance constraints.
Then propose a simple logistics plan.

Rules:
- If car_ownership_ratio == 0 and distance_bucket is 5-15 or 15-30, do NOT assign a driver.
  Instead suggest transit/meetpoint/rideshare.
- Otherwise, you may assign driver(s) but keep it simple.

Output schema:
{
 "options":[{"name":str,"why":str,"price":str,"distance_miles":number,"category":str}],
 "logistics":{"plan":str,"driver_assignment":[{"member_index":int,"drives":bool}]}
}
"""

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "options": {
            "type": "array",
            "minItems": 3,
            "maxItems": 3,
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "why": {"type": "string"},
                    "price": {"type": "string"},
                    "distance_miles": {"type": "number"},
                    "category": {"type": "string"},
                },
                "required": ["name", "why", "price", "distance_miles", "category"],
            },
        },
        "logistics": {
            "type": "object",
            "properties": {
                "plan": {"type": "string"},
                "driver_assignment": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "member_index": {"type": "integer"},
                            "drives": {"type": "boolean"},
                        },
                        "required": ["member_index", "drives"],
                    },
                },
            },
            "required": ["plan", "driver_assignment"],
        },
    },
    "required": ["options", "logistics"],
}


def budget_allows(price: str, budget_tier: str) -> bool:
    if budget_tier == "low":
        return price in ["$", "$$"]
    if budget_tier == "med":
        return price in ["$", "$$", "$$$"]
    return True


def make_venues(city_tier: str, budget_tier: str, distance_bucket: str) -> list[dict[str, Any]]:
    base_n = {"big": 18, "mid": 10, "small": 5}[city_tier]
    venues = []

    for _ in range(base_n):
        category = random.choice(CATEGORIES)
        price = random.choice(PRICE_LEVELS)

        if distance_bucket == "0-5":
            distance = round(random.uniform(0.2, 4.9), 1)
        elif distance_bucket == "5-15":
            distance = round(random.uniform(5.0, 14.9), 1)
        else:
            distance = round(random.uniform(15.0, 29.9), 1)

        has_hours = True
        if city_tier == "small" and random.random() < 0.35:
            has_hours = False

        # Intentionally bias sparse low-budget slices toward pricier options.
        if city_tier == "small" and budget_tier == "low" and random.random() < 0.40:
            price = "$$$"

        venues.append(
            {
                "name": f"{category.title()} Spot {random.randint(1, 99)}",
                "price": price,
                "distance_miles": distance,
                "categories": [category],
                "has_hours": has_hours,
            }
        )
    return venues


def generate_sample() -> dict[str, Any]:
    city_tier = random.choices(["big", "mid", "small"], weights=[0.45, 0.35, 0.20])[0]
    budget_tier = random.choices(["low", "med", "high"], weights=[0.35, 0.50, 0.15])[0]
    distance_bucket = random.choices(["0-5", "5-15", "15-30"], weights=[0.55, 0.30, 0.15])[0]
    group_size = random.randint(2, 8)
    car_ownership_ratio = random.choice([0.0, 0.25, 0.5, 0.75, 1.0])
    dietary = random.choices(
        [[], ["vegan"], ["gluten_free"], ["vegan", "gluten_free"]],
        weights=[0.55, 0.20, 0.20, 0.05],
    )[0]
    vibe = random.sample(["chill", "loud", "indoors", "outdoors", "active", "low_key"], k=2)

    return {
        "cycle_id": str(uuid.uuid4()),
        "city_tier": city_tier,
        "budget_tier": budget_tier,
        "car_ownership_ratio": car_ownership_ratio,
        "distance_bucket": distance_bucket,
        "group_size": group_size,
        "dietary": dietary,
        "vibe": vibe,
        "tool_snapshot": {"venues": make_venues(city_tier, budget_tier, distance_bucket)},
    }


def bucket_car_ratio(value: float) -> str:
    if value == 0.0:
        return "none"
    if value <= 0.25:
        return "low"
    if value <= 0.75:
        return "mid"
    return "high"


def build_user_prompt(sample: dict[str, Any]) -> str:
    return json.dumps(
        {
            "constraints": {
                "budget_tier": sample["budget_tier"],
                "distance_bucket": sample["distance_bucket"],
                "car_ownership_ratio": sample["car_ownership_ratio"],
                "dietary": sample["dietary"],
                "vibe": sample["vibe"],
                "group_size": sample["group_size"],
            },
            "venues": sample["tool_snapshot"]["venues"],
        }
    )


def parse_json_safe(text: str) -> dict[str, Any] | None:
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except Exception:
                return None
        return None


def call_vllm_chat(
    session: requests.Session,
    base_url: str,
    model: str,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    use_schema: bool,
) -> tuple[str, bool]:
    url = f"{base_url.rstrip('/')}/v1/chat/completions"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    if use_schema:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "ketchup-plan", "schema": OUTPUT_SCHEMA},
        }

    response = request_with_retries(session, "post", url, json=payload, timeout=90)
    if response.status_code != 200 and use_schema:
        payload.pop("response_format", None)
        retry_response = request_with_retries(session, "post", url, json=payload, timeout=90)
        if retry_response.status_code != 200:
            retry_response.raise_for_status()
        data = retry_response.json()
        return data["choices"][0]["message"]["content"], False

    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"], use_schema


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    attempts: int = 8,
    backoff_seconds: float = 5.0,
    **kwargs: Any,
) -> Response:
    retriable_statuses = {404, 429, 500, 502, 503, 504}
    last_exception: Exception | None = None
    last_response: Response | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = session.request(method, url, **kwargs)
            if response.status_code not in retriable_statuses:
                return response
            last_response = response
        except requests.RequestException as exc:
            last_exception = exc

        if attempt < attempts:
            time.sleep(backoff_seconds * attempt)

    if last_response is not None:
        return last_response
    if last_exception is not None:
        raise last_exception
    raise RuntimeError(f"Request failed without response or exception for {method.upper()} {url}")


def score(sample: dict[str, Any], output: dict[str, Any] | None) -> dict[str, Any]:
    result = {
        "json_valid": 0,
        "options_count_ok": 0,
        "budget_compliance": 0.0,
        "distance_compliance": 0.0,
        "logistics_feasible": 0,
    }
    if output is None:
        return result

    options = output.get("options", [])
    logistics = output.get("logistics", {})
    result["json_valid"] = 1
    result["options_count_ok"] = 1 if isinstance(options, list) and len(options) == 3 else 0

    max_distance = {"0-5": 5.0, "5-15": 15.0, "15-30": 30.0}[sample["distance_bucket"]]

    budget_ok = 0
    distance_ok = 0
    for option in options[:3]:
        price = str(option.get("price", ""))
        try:
            distance = float(option.get("distance_miles", 999))
        except Exception:
            distance = 999.0

        if budget_allows(price, sample["budget_tier"]):
            budget_ok += 1
        if distance <= max_distance:
            distance_ok += 1

    if isinstance(options, list) and len(options) >= 3:
        result["budget_compliance"] = budget_ok / 3.0
        result["distance_compliance"] = distance_ok / 3.0

    plan_text = str(logistics.get("plan") or "").lower()
    if sample["car_ownership_ratio"] == 0.0 and sample["distance_bucket"] in ["5-15", "15-30"]:
        if any(token in plan_text for token in ["transit", "subway", "bus", "meet", "meetpoint", "rideshare", "uber", "lyft"]):
            result["logistics_feasible"] = 1
    else:
        result["logistics_feasible"] = 1

    return result


def print_overall(df: pd.DataFrame) -> None:
    cols = [
        "json_valid",
        "options_count_ok",
        "budget_compliance",
        "distance_compliance",
        "logistics_feasible",
    ]
    print("\n=== Overall ===")
    print(df[cols].mean())


def print_slices(df: pd.DataFrame) -> None:
    print("\n=== Slice: city_tier x budget_tier ===")
    slice_df = (
        df.groupby(["city_tier", "budget_tier"])
        .agg(
            n=("cycle_id", "count"),
            json_valid=("json_valid", "mean"),
            budget_compliance=("budget_compliance", "mean"),
            logistics_feasible=("logistics_feasible", "mean"),
        )
        .reset_index()
    )
    print(slice_df.sort_values(["budget_compliance", "logistics_feasible"]).to_string(index=False))

    print("\n=== Slice: distance_bucket x car_ratio_bucket ===")
    distance_df = (
        df.groupby(["distance_bucket", "car_ratio_bucket"])
        .agg(
            n=("cycle_id", "count"),
            logistics_feasible=("logistics_feasible", "mean"),
            distance_compliance=("distance_compliance", "mean"),
        )
        .reset_index()
    )
    print(distance_df.sort_values(["logistics_feasible", "distance_compliance"]).to_string(index=False))

    print("\n=== Worst slices (n>=8) by budget_compliance ===")
    worst_df = (
        df.groupby(["city_tier", "budget_tier", "distance_bucket", "car_ratio_bucket"])
        .agg(
            n=("cycle_id", "count"),
            budget_compliance=("budget_compliance", "mean"),
            json_valid=("json_valid", "mean"),
            logistics_feasible=("logistics_feasible", "mean"),
        )
        .reset_index()
    )
    worst_df = worst_df[worst_df["n"] >= 8].sort_values("budget_compliance").head(12)
    if worst_df.empty:
        print("(none with n>=8; reduce threshold or increase sample count)")
        return
    print(worst_df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=os.getenv("VLLM_BASE_URL", "http://localhost:8080"))
    parser.add_argument("--model", default=os.getenv("VLLM_MODEL", "Qwen/Qwen3-4B-Instruct-2507"))
    parser.add_argument("--n", type=int, default=int(os.getenv("N_SAMPLES", "200")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("SEED", "25")))
    parser.add_argument("--temperature", type=float, default=float(os.getenv("TEMP", "0.2")))
    parser.add_argument("--max-tokens", type=int, default=int(os.getenv("MAX_TOKENS", "4096")))
    parser.add_argument("--progress-every", type=int, default=int(os.getenv("PROGRESS_EVERY", "5")))
    parser.add_argument("--no-schema", action="store_true")
    parser.add_argument(
        "--save-csv",
        default=os.getenv("SAVE_CSV", "data/reports/model_bias_results.csv"),
        help="Path to save per-sample results CSV.",
    )
    parser.add_argument(
        "--print-failures",
        type=int,
        default=int(os.getenv("PRINT_FAILURES", "2")),
        help="Print raw model outputs for the first N parse failures.",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    session = requests.Session()

    try:
        response = request_with_retries(
            session,
            "get",
            f"{args.base_url.rstrip('/')}/v1/models",
            timeout=30,
        )
        if response.status_code == 200:
            print(f"[INFO] /v1/models reachable. {len(response.json().get('data', []))} model(s) advertised.")
    except Exception as exc:
        print(f"[WARN] Could not reach /v1/models: {exc!r}. Continuing anyway.")

    rows: list[dict[str, Any]] = []
    started = time.time()
    ok_calls = 0
    err_calls = 0
    schema_on = not args.no_schema
    parse_fail_printed = 0

    for idx in range(1, args.n + 1):
        sample = generate_sample()
        try:
            response_text, schema_on = call_vllm_chat(
                session=session,
                base_url=args.base_url,
                model=args.model,
                system=SYSTEM_PROMPT,
                user=build_user_prompt(sample),
                temperature=args.temperature,
                max_tokens=args.max_tokens,
                use_schema=schema_on,
            )
            output = parse_json_safe(response_text)
            if output is None and parse_fail_printed < args.print_failures:
                print("\n[DEBUG] Parse failure raw output:")
                print(response_text[:800])
                print("-----")
                parse_fail_printed += 1
            ok_calls += 1
        except Exception as exc:
            output = None
            err_calls += 1
            if err_calls <= 3:
                print(f"[ERROR] Call failed (showing first 3): {exc!r}")

        metrics = score(sample, output)
        rows.append(
            {
                "cycle_id": sample["cycle_id"],
                "city_tier": sample["city_tier"],
                "budget_tier": sample["budget_tier"],
                "distance_bucket": sample["distance_bucket"],
                "group_size": sample["group_size"],
                "car_ratio_bucket": bucket_car_ratio(sample["car_ownership_ratio"]),
                "dietary_flag": "yes" if sample["dietary"] else "no",
                **metrics,
            }
        )

        if idx % args.progress_every == 0 or idx == args.n:
            temp_df = pd.DataFrame(rows)
            means = temp_df[["json_valid", "budget_compliance", "logistics_feasible"]].mean()
            elapsed = time.time() - started
            print(
                f"[PROGRESS] {idx}/{args.n} | ok={ok_calls} err={err_calls} | "
                f"json_valid={means['json_valid']:.2f} "
                f"budget={means['budget_compliance']:.2f} "
                f"logistics={means['logistics_feasible']:.2f} | "
                f"elapsed={elapsed:.1f}s | schema={'ON' if schema_on else 'OFF'}"
            )

    df = pd.DataFrame(rows)
    print_overall(df)
    print_slices(df)

    if args.save_csv:
        output_path = Path(args.save_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n[INFO] Saved results to: {output_path}")


if __name__ == "__main__":
    main()

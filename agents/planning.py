"""Canonical planning agent orchestration with vLLM tool-calling."""

import ast
import json
import logging
from math import ceil
import re
from datetime import datetime, timedelta
from typing import Any, Callable
from urllib.parse import urlparse
from uuid import UUID

import httpx
from openai import APIConnectionError, AsyncOpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from config import get_settings
from database import db

logger = logging.getLogger(__name__)

# Shared AsyncOpenAI client for connection pooling.
_planner_client: AsyncOpenAI | None = None

# Tool schema exposed to the LLM.
PLANNER_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_directions",
            "description": "Get travel distance and duration between an origin and destination.",
            "parameters": {
                "type": "object",
                "properties": {
                    "origin": {"type": "string", "description": "Starting address or place."},
                    "destination": {
                        "type": "string",
                        "description": "Destination address or place.",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["driving", "transit", "walking"],
                        "description": "Travel mode.",
                    },
                },
                "required": ["origin", "destination"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_places",
            "description": "Search venues near a location.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Venue type query, e.g. 'bowling alley'.",
                    },
                    "location": {
                        "type": "string",
                        "description": "Area to search near, e.g. 'Boston, MA'.",
                    },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 3,
                    },
                },
                "required": ["query", "location"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                "Search the public web for activity ideas or venues. "
                "Use when maps search yields no useful venue results."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query, e.g. 'indoor activities for groups'.",
                    },
                    "location": {
                        "type": "string",
                        "description": "Optional location context, e.g. 'Boston, MA'.",
                    },
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
]

SYSTEM_PROMPT_TOOL_GROUNDED = (
    "You are Ketchup's planning engine. Build exactly 5 plans for a friend group. "
    "Use tools to ground recommendations in real places and travel times. "
    "Return strict JSON only with key 'plans'."
)

SYSTEM_PROMPT_BEST_EFFORT = (
    "You are Ketchup's planning engine. Build exactly 5 plans for a friend group. "
    "Tooling may be unavailable; do not mention missing tools, integrations, or API keys. "
    "Return strict JSON only with key 'plans'."
)

DEFAULT_VIBES = ["anchor", "pivot", "reach", "chill", "wildcard"]
MAX_TOOL_ROUNDS = 2
MAX_COMPLETION_TOKENS = 512
REPAIR_MAX_COMPLETION_TOKENS = 192
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_FIELD_MASK = (
    "places.displayName,places.formattedAddress,places.rating,places.priceLevel"
)
ROUTES_COMPUTE_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
ROUTES_FIELD_MASK = (
    "routes.distanceMeters,routes.duration,routes.legs.distanceMeters,routes.legs.duration"
)
TAVILY_SEARCH_URL = "https://api.tavily.com/search"


class PlannerError(Exception):
    """Raised when plan generation cannot complete."""


async def init_planner_client() -> None:
    """Initialize and cache the AsyncOpenAI client for vLLM."""
    global _planner_client
    if _planner_client is not None:
        return

    settings = get_settings()
    timeout = httpx.Timeout(
        connect=settings.vllm_connect_timeout_seconds,
        read=settings.vllm_read_timeout_seconds,
        write=settings.vllm_write_timeout_seconds,
        pool=settings.vllm_pool_timeout_seconds,
    )

    http_client = httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=settings.vllm_max_connections,
            max_keepalive_connections=settings.vllm_max_keepalive_connections,
        )
    )

    _planner_client = AsyncOpenAI(
        base_url=settings.vllm_base_url,
        api_key=settings.vllm_api_key,
        timeout=timeout,
        max_retries=0,
        http_client=http_client,
    )


async def close_planner_client() -> None:
    """Close shared AsyncOpenAI client."""
    global _planner_client
    if _planner_client is not None:
        await _planner_client.close()
        _planner_client = None


def _get_planner_client() -> AsyncOpenAI:
    if _planner_client is None:
        raise PlannerError("Planner client not initialized")
    return _planner_client


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10, jitter=2),
    retry=retry_if_exception_type(
        (
            APIConnectionError,
            httpx.ConnectTimeout,
            httpx.ConnectError,
            httpx.PoolTimeout,
        )
    ),
)
async def _call_vllm_chat(messages: list[dict[str, Any]], **kwargs):
    client = _get_planner_client()
    settings = get_settings()

    return await client.chat.completions.create(
        model=settings.vllm_model,
        messages=messages,
        **kwargs,
    )


def _strip_code_fence(text: str) -> str:
    candidate = text.strip()
    # Some model outputs include reasoning traces; remove them before JSON parsing.
    candidate = re.sub(r"<think>.*?</think>", "", candidate, flags=re.DOTALL | re.IGNORECASE).strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```[a-zA-Z0-9_\-]*", "", candidate).strip()
        if candidate.endswith("```"):
            candidate = candidate[:-3].strip()
    return candidate


def _extract_balanced_segment(text: str, start_idx: int) -> str | None:
    opener = text[start_idx]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escape = False

    for idx in range(start_idx, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return text[start_idx : idx + 1]

    return None


def _extract_json_candidate(text: str) -> str | None:
    for idx, ch in enumerate(text):
        if ch not in "{[":
            continue
        candidate = _extract_balanced_segment(text, idx)
        if candidate:
            return candidate
    return None


def _sanitize_json_like(candidate: str) -> str:
    # Remove trailing commas that some models add before object/array close.
    return re.sub(r",\s*([}\]])", r"\1", candidate)


def _parse_json_like(text: str) -> Any:
    candidate = text.strip()
    if not candidate:
        raise PlannerError("LLM output was empty")

    # 1) Strict JSON first.
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass

    # 2) Try JSON candidate substring.
    substring = _extract_json_candidate(candidate)
    if substring:
        cleaned = _sanitize_json_like(substring)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            # 3) Python-literal fallback (single quotes/None/True/False).
            try:
                return ast.literal_eval(cleaned)
            except (ValueError, SyntaxError) as exc:
                raise PlannerError("LLM output JSON parse failed") from exc

    # 4) Full-text Python-literal fallback.
    try:
        return ast.literal_eval(candidate)
    except (ValueError, SyntaxError) as exc:
        raise PlannerError("LLM output was not valid JSON") from exc


def _parse_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _clamp_novelty_target(value: Any, default: float = 0.5) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, min(1.0, parsed))


def _normalize_venue_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text)


def _prior_venue_set(prior_venues: list[str] | None) -> set[str]:
    if not prior_venues:
        return set()
    return {
        token
        for token in (_normalize_venue_token(v) for v in prior_venues)
        if token
    }


def _select_with_novelty(
    items: list[dict[str, Any]],
    prior_venues: list[str] | None,
    novelty_target: float,
    label_getter: Callable[[dict[str, Any]], str],
    max_items: int = 5,
) -> list[dict[str, Any]]:
    if not items:
        return []

    prior_set = _prior_venue_set(prior_venues)
    required_novel = min(max_items, int(ceil(max_items * _clamp_novelty_target(novelty_target))))

    novel: list[dict[str, Any]] = []
    repeated: list[dict[str, Any]] = []
    for item in items:
        label = _normalize_venue_token(label_getter(item))
        if label and label in prior_set:
            repeated.append(item)
        else:
            novel.append(item)

    selected: list[dict[str, Any]] = []
    selected.extend(novel[:required_novel])

    for item in [*novel[required_novel:], *repeated]:
        if len(selected) >= max_items:
            break
        selected.append(item)

    if len(selected) < max_items:
        for item in items:
            if len(selected) >= max_items:
                break
            if item not in selected:
                selected.append(item)

    return selected[:max_items]


def _normalize_plan(raw: dict[str, Any], idx: int) -> dict[str, Any]:
    vibe = raw.get("vibe_type")
    if vibe not in DEFAULT_VIBES:
        vibe = DEFAULT_VIBES[min(idx, len(DEFAULT_VIBES) - 1)]

    logistics = raw.get("logistics")
    if not isinstance(logistics, dict):
        logistics = {}

    return {
        "title": str(raw.get("title") or f"Plan Option {idx + 1}"),
        "description": raw.get("description") or "",
        "vibe_type": vibe,
        "date_time": _parse_datetime(raw.get("date_time")),
        "location": raw.get("location") or "",
        "venue_name": raw.get("venue_name") or raw.get("title") or "",
        "estimated_cost": raw.get("estimated_cost") or "",
        "logistics": logistics,
    }


def _extract_plans(raw_text: str) -> list[dict[str, Any]]:
    text = _strip_code_fence(raw_text)

    parsed: Any = _parse_json_like(text)

    raw_plans: list[Any]
    if isinstance(parsed, dict):
        raw_plans = parsed.get("plans") or []
    elif isinstance(parsed, list):
        raw_plans = parsed
    else:
        raw_plans = []

    if not raw_plans:
        raise PlannerError("LLM returned no plans")

    plans: list[dict[str, Any]] = []
    for idx, item in enumerate(raw_plans[:5]):
        if isinstance(item, dict):
            plans.append(_normalize_plan(item, idx))

    if not plans:
        raise PlannerError("LLM returned malformed plans")

    while len(plans) < 5:
        plans.append(
            _normalize_plan(
                {
                    "title": f"Plan Option {len(plans) + 1}",
                    "description": "Fallback option generated due to incomplete model response.",
                    "vibe_type": DEFAULT_VIBES[len(plans)],
                    "location": "",
                    "estimated_cost": "",
                    "logistics": {},
                },
                len(plans),
            )
        )

    return plans


def _format_member(member: dict[str, Any]) -> str:
    likes = member.get("activity_likes") or []
    dislikes = member.get("activity_dislikes") or []
    likes_s = ", ".join(likes) if likes else "none"
    dislikes_s = ", ".join(dislikes) if dislikes else "none"
    return (
        f"- {member.get('name') or member.get('email')}: "
        f"location={member.get('default_location') or 'unknown'}, "
        f"budget={member.get('budget_preference') or 'unspecified'}, "
        f"likes={likes_s}, dislikes={dislikes_s}"
    )


def _base_location_from_context(context: dict[str, Any]) -> str:
    for member in context["members"]:
        location = member.get("default_location")
        if location:
            return str(location)
    return "Boston, MA"


def _build_fallback_plans(
    context: dict[str, Any], reason: str, refinement_notes: str | None = None
) -> list[dict[str, Any]]:
    base_location = _base_location_from_context(context)

    member_names = [
        str(member.get("name") or member.get("email") or "member")
        for member in context["members"]
    ]
    group_name = context["group"]["name"]
    reason_short = reason[:180]
    refinement_short = (refinement_notes or "")[:240]

    templates = [
        ("Cozy Cafe Catch-up", "Relaxed hangout over coffee and conversation.", "$10-20 per person"),
        ("Food Hall Sampler", "Try multiple cuisines together in one spot.", "$20-35 per person"),
        ("Park Picnic Sunset", "Low-cost outdoor plan with time to talk.", "$5-15 per person"),
        ("Bowling + Snacks", "Casual activity with light competition.", "$25-40 per person"),
        ("Live Event Night", "Explore a slightly adventurous local event.", "$30-60 per person"),
    ]

    plans: list[dict[str, Any]] = []
    for idx, (title, description, cost) in enumerate(templates):
        plans.append(
            {
                "title": title,
                "description": f"{description} (Fallback plan for {group_name}.)",
                "vibe_type": DEFAULT_VIBES[idx],
                "date_time": datetime.utcnow() + timedelta(days=7 + idx),
                "location": base_location,
                "venue_name": title,
                "estimated_cost": cost,
                "logistics": {
                    "source": "fallback",
                    "reason": reason_short,
                    "refinement_notes": refinement_short,
                    "members": member_names,
                },
            }
        )
    return plans


def _cost_from_price_level(price_level: Any) -> str:
    if isinstance(price_level, str):
        enum_map = {
            "PRICE_LEVEL_FREE": 0,
            "PRICE_LEVEL_INEXPENSIVE": 1,
            "PRICE_LEVEL_MODERATE": 2,
            "PRICE_LEVEL_EXPENSIVE": 3,
            "PRICE_LEVEL_VERY_EXPENSIVE": 4,
        }
        mapped = enum_map.get(price_level.strip().upper())
        if mapped is not None:
            price_level = mapped
    try:
        level = int(price_level)
    except (TypeError, ValueError):
        return "$20-40 per person"

    if level <= 0:
        return "$0-10 per person"
    if level == 1:
        return "$10-20 per person"
    if level == 2:
        return "$20-40 per person"
    if level == 3:
        return "$40-80 per person"
    return "$80+ per person"


def _duration_to_seconds(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None

    token = value.strip()
    if token.endswith("s"):
        token = token[:-1]
    try:
        return float(token)
    except ValueError:
        return None


def _format_duration(seconds: float | None) -> str | None:
    if seconds is None or seconds <= 0:
        return None
    minutes = max(1, int(round(seconds / 60.0)))
    if minutes < 60:
        return f"{minutes} min"
    hours, rem = divmod(minutes, 60)
    if rem == 0:
        return f"{hours} hr"
    return f"{hours} hr {rem} min"


def _format_distance(meters: Any) -> str | None:
    try:
        meters_value = float(meters)
    except (TypeError, ValueError):
        return None
    if meters_value <= 0:
        return None

    miles = meters_value / 1609.344
    if miles < 0.2:
        feet = int(round(meters_value * 3.28084))
        return f"{max(1, feet)} ft"
    return f"{miles:.1f} mi"


def _extract_places_from_tool_messages(tool_messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    places: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for message in tool_messages:
        if message.get("role") != "tool":
            continue
        content = message.get("content")
        if not isinstance(content, str):
            continue
        try:
            payload = json.loads(content)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        payload_places = payload.get("places")
        if not isinstance(payload_places, list):
            continue

        for place in payload_places:
            if not isinstance(place, dict):
                continue
            name = str(place.get("name") or "").strip()
            address = str(place.get("address") or "").strip()
            if not name and not address:
                continue

            key = (name.lower(), address.lower())
            if key in seen:
                continue
            seen.add(key)

            places.append(
                {
                    "name": name,
                    "address": address,
                    "rating": place.get("rating"),
                    "price_level": place.get("price_level"),
                }
            )
    return places


def _build_maps_grounded_fallback_plans_from_places(
    context: dict[str, Any],
    places: list[dict[str, Any]],
    reason: str,
    refinement_notes: str | None = None,
    prior_venues: list[str] | None = None,
    novelty_target: float = 0.5,
) -> list[dict[str, Any]] | None:
    if not places:
        return None

    base_location = _base_location_from_context(context)

    member_names = [
        str(member.get("name") or member.get("email") or "member")
        for member in context["members"]
    ]
    reason_short = reason[:180]
    refinement_short = (refinement_notes or "")[:240]
    prior_set = _prior_venue_set(prior_venues)
    selected_places = _select_with_novelty(
        items=places,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
        label_getter=lambda place: str(place.get("name") or ""),
        max_items=5,
    )

    plans: list[dict[str, Any]] = []
    for idx, place in enumerate(selected_places):
        venue_name = place["name"] or f"Local Option {idx + 1}"
        location = place["address"] or base_location
        rating = place.get("rating")
        rating_text = f" Rated {rating}/5." if rating is not None else ""
        venue_token = _normalize_venue_token(venue_name)
        plans.append(
            {
                "title": venue_name,
                "description": f"Meet at {venue_name} in {location}.{rating_text}",
                "vibe_type": DEFAULT_VIBES[idx],
                "date_time": datetime.utcnow() + timedelta(days=7 + idx),
                "location": location,
                "venue_name": venue_name,
                "estimated_cost": _cost_from_price_level(place.get("price_level")),
                "logistics": {
                    "source": "maps_fallback",
                    "reason": reason_short,
                    "refinement_notes": refinement_short,
                    "members": member_names,
                    "novelty_target": _clamp_novelty_target(novelty_target),
                    "is_novel": bool(venue_token) and venue_token not in prior_set,
                    "venue": place,
                },
            }
        )

    if len(plans) < 5:
        generic = _build_fallback_plans(
            context=context,
            reason=reason,
            refinement_notes=refinement_notes,
        )
        for plan in generic:
            if len(plans) >= 5:
                break
            plans.append(plan)

    return plans[:5]


def _build_maps_grounded_fallback_plans(
    context: dict[str, Any],
    tool_messages: list[dict[str, Any]],
    reason: str,
    refinement_notes: str | None = None,
    prior_venues: list[str] | None = None,
    novelty_target: float = 0.5,
) -> list[dict[str, Any]] | None:
    places = _extract_places_from_tool_messages(tool_messages)
    return _build_maps_grounded_fallback_plans_from_places(
        context=context,
        places=places,
        reason=reason,
        refinement_notes=refinement_notes,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
    )


def _extract_web_results_from_tool_messages(
    tool_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for message in tool_messages:
        if message.get("role") != "tool":
            continue
        content = message.get("content")
        if not isinstance(content, str):
            continue
        try:
            payload = json.loads(content)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        payload_results = payload.get("results")
        if not isinstance(payload_results, list):
            continue

        for item in payload_results:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            link = str(item.get("link") or "").strip()
            snippet = str(item.get("snippet") or "").strip()
            source = str(item.get("source") or "").strip()
            if not title and not link:
                continue

            key = (title.lower(), link.lower())
            if key in seen:
                continue
            seen.add(key)

            results.append(
                {
                    "title": title,
                    "link": link,
                    "snippet": snippet,
                    "source": source,
                }
            )
    return results


def _build_web_grounded_fallback_plans(
    context: dict[str, Any],
    web_results: list[dict[str, Any]],
    reason: str,
    refinement_notes: str | None = None,
    prior_venues: list[str] | None = None,
    novelty_target: float = 0.5,
) -> list[dict[str, Any]] | None:
    if not web_results:
        return None

    base_location = _base_location_from_context(context)
    member_names = [
        str(member.get("name") or member.get("email") or "member")
        for member in context["members"]
    ]
    reason_short = reason[:180]
    refinement_short = (refinement_notes or "")[:240]
    prior_set = _prior_venue_set(prior_venues)
    selected_results = _select_with_novelty(
        items=web_results,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
        label_getter=lambda result: str(result.get("title") or ""),
        max_items=5,
    )

    plans: list[dict[str, Any]] = []
    for idx, result in enumerate(selected_results):
        title = result.get("title") or f"Web Option {idx + 1}"
        snippet = result.get("snippet") or "Candidate discovered from web search."
        source = result.get("source") or result.get("link") or "web"
        title_token = _normalize_venue_token(title)
        plans.append(
            {
                "title": str(title)[:120],
                "description": f"{str(snippet)[:260]}",
                "vibe_type": DEFAULT_VIBES[idx],
                "date_time": datetime.utcnow() + timedelta(days=7 + idx),
                "location": base_location,
                "venue_name": str(title)[:120],
                "estimated_cost": "$20-40 per person",
                "logistics": {
                    "source": "web_fallback",
                    "reason": reason_short,
                    "refinement_notes": refinement_short,
                    "members": member_names,
                    "novelty_target": _clamp_novelty_target(novelty_target),
                    "is_novel": bool(title_token) and title_token not in prior_set,
                    "reference": {
                        "title": result.get("title"),
                        "link": result.get("link"),
                        "source": source,
                    },
                },
            }
        )

    if len(plans) < 5:
        generic = _build_fallback_plans(
            context=context,
            reason=reason,
            refinement_notes=refinement_notes,
        )
        for plan in generic:
            if len(plans) >= 5:
                break
            plans.append(plan)

    return plans[:5]


def _build_web_fallback_queries(
    context: dict[str, Any],
    refinement_notes: str | None = None,
    refinement_descriptors: list[str] | None = None,
) -> list[str]:
    activity_seeds: list[str] = []
    seen = set()
    for member in context["members"]:
        likes = member.get("activity_likes") or []
        if not isinstance(likes, list):
            continue
        for like in likes:
            like_s = str(like).strip()
            if not like_s:
                continue
            key = like_s.lower()
            if key in seen:
                continue
            seen.add(key)
            activity_seeds.append(like_s)
            if len(activity_seeds) >= 3:
                break
        if len(activity_seeds) >= 3:
            break

    if not activity_seeds:
        activity_seeds = ["things to do", "group activities", "friendly events"]

    if refinement_notes:
        activity_seeds.append("activities matching group voting feedback")

    descriptor_seeds = {
        "budget_friendly": "affordable activities",
        "short_travel": "activities close to downtown",
        "more_active": "active group activities",
        "more_chill": "quiet hangout spots",
        "indoor": "indoor activities",
        "outdoor": "outdoor activities",
        "food_focused": "group dining experiences",
        "accessible": "easy access group activities",
    }
    for descriptor in refinement_descriptors or []:
        seed = descriptor_seeds.get(descriptor)
        if seed:
            activity_seeds.append(seed)

    return [f"{seed} for friends" for seed in activity_seeds[:3]]


def _build_maps_fallback_queries(
    context: dict[str, Any],
    refinement_notes: str | None = None,
    refinement_descriptors: list[str] | None = None,
) -> list[str]:
    activity_seeds: list[str] = []
    seen = set()
    for member in context["members"]:
        likes = member.get("activity_likes") or []
        if not isinstance(likes, list):
            continue
        for like in likes:
            like_s = str(like).strip()
            if not like_s:
                continue
            key = like_s.lower()
            if key in seen:
                continue
            seen.add(key)
            activity_seeds.append(like_s)
            if len(activity_seeds) >= 4:
                break
        if len(activity_seeds) >= 4:
            break

    if not activity_seeds:
        activity_seeds = ["group activities", "casual dinner", "indoor activities"]

    if refinement_notes:
        activity_seeds.append("activities matching group voting feedback")

    descriptor_seeds = {
        "budget_friendly": "cheap eats",
        "short_travel": "near city center",
        "more_active": "sports activity",
        "more_chill": "cafe",
        "indoor": "indoor activity",
        "outdoor": "outdoor park activity",
        "food_focused": "restaurant",
        "accessible": "easy access venue",
    }
    for descriptor in refinement_descriptors or []:
        seed = descriptor_seeds.get(descriptor)
        if seed:
            activity_seeds.append(seed)

    return activity_seeds[:4]


async def _run_deterministic_maps_fallback_search(
    context: dict[str, Any],
    refinement_notes: str | None = None,
    refinement_descriptors: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    base_location = _base_location_from_context(context)
    queries = _build_maps_fallback_queries(
        context,
        refinement_notes=refinement_notes,
        refinement_descriptors=refinement_descriptors,
    )
    places: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    errors: list[str] = []

    for query in queries:
        payload = await _search_places(query=query, location=base_location, max_results=4)
        if payload.get("error"):
            details = str(payload.get("details") or payload["error"])
            errors.append(f"{payload['error']} ({details[:120]})")
            continue

        raw_places = payload.get("places")
        if not isinstance(raw_places, list):
            continue

        for item in raw_places:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            address = str(item.get("address") or "").strip()
            if not name and not address:
                continue
            key = (name.lower(), address.lower())
            if key in seen:
                continue
            seen.add(key)
            places.append(
                {
                    "name": name,
                    "address": address,
                    "rating": item.get("rating"),
                    "price_level": item.get("price_level"),
                }
            )
            if len(places) >= 8:
                return places, errors

    return places, errors


def _summarize_tool_results(tool_messages: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "tool_calls": 0,
        "place_calls": 0,
        "place_results": 0,
        "web_calls": 0,
        "web_results": 0,
        "errors": [],
    }

    for message in tool_messages:
        if message.get("role") != "tool":
            continue
        summary["tool_calls"] += 1

        content = message.get("content")
        if not isinstance(content, str):
            continue

        try:
            payload = json.loads(content)
        except json.JSONDecodeError:
            summary["errors"].append("Tool payload was not valid JSON")
            continue

        if not isinstance(payload, dict):
            continue

        error = payload.get("error")
        if isinstance(error, str) and error:
            summary["errors"].append(error)

        places = payload.get("places")
        if isinstance(places, list):
            summary["place_calls"] += 1
            summary["place_results"] += len(places)

        web_results = payload.get("results")
        if isinstance(web_results, list):
            summary["web_calls"] += 1
            summary["web_results"] += len(web_results)

    return summary


async def _load_group_context(group_id: UUID) -> dict[str, Any]:
    group = await db.fetchrow(
        "SELECT id, name FROM groups WHERE id = $1",
        group_id,
    )
    if not group:
        raise PlannerError("Group not found")

    members = await db.fetch(
        """
        SELECT
            u.id,
            u.name,
            u.email,
            gp.default_location,
            gp.activity_likes,
            gp.activity_dislikes,
            gp.budget_preference,
            gp.notes
        FROM group_members gm
        JOIN users u ON u.id = gm.user_id
        LEFT JOIN group_preferences gp ON gp.group_id = gm.group_id AND gp.user_id = gm.user_id
        WHERE gm.group_id = $1 AND gm.status = 'active'
        ORDER BY u.name NULLS LAST, u.email
        """,
        group_id,
    )

    recent_events = await db.fetch(
        """
        SELECT p.title, e.event_date
        FROM events e
        JOIN plans p ON p.id = e.plan_id
        WHERE e.group_id = $1
        ORDER BY e.event_date DESC
        LIMIT 5
        """,
        group_id,
    )

    return {
        "group": dict(group),
        "members": [dict(m) for m in members],
        "recent_events": [dict(e) for e in recent_events],
    }


def _build_prompt(
    context: dict[str, Any],
    refinement_notes: str | None = None,
    require_tool_grounding: bool = True,
    web_search_enabled: bool = False,
    novelty_target: float = 0.5,
    prior_venues: list[str] | None = None,
    refinement_descriptors: list[str] | None = None,
    refinement_focus_note: str | None = None,
) -> str:
    member_lines = "\n".join(_format_member(m) for m in context["members"])

    if context["recent_events"]:
        history_lines = "\n".join(
            f"- {e['title']} at {e['event_date'].isoformat() if e.get('event_date') else 'unknown'}"
            for e in context["recent_events"]
        )
    else:
        history_lines = "- No recent events"

    refinement_block = ""
    if refinement_notes:
        refinement_block = f"\nVoting feedback to consider:\n{refinement_notes}\n"

    descriptor_block = ""
    descriptors = [str(d).strip() for d in (refinement_descriptors or []) if str(d).strip()]
    if descriptors:
        descriptor_block = (
            "\nRefinement descriptors selected by lead:\n"
            + "\n".join(f"- {descriptor}" for descriptor in descriptors)
            + "\n"
        )

    lead_focus_block = ""
    if refinement_focus_note:
        lead_focus_block = f"\nLead focus note:\n{str(refinement_focus_note).strip()}\n"

    novelty_pct = int(round(_clamp_novelty_target(novelty_target) * 100))
    novelty_block = (
        f"Novelty target: aim for at least {novelty_pct}% of plans using venues/ideas "
        "not in the prior venue list when feasible."
    )

    prior_venues_block = ""
    if prior_venues:
        prior_venues_block = (
            "\nPrior venues to avoid repeating unless necessary:\n"
            + "\n".join(f"- {venue}" for venue in prior_venues[:25])
            + "\n"
        )

    if require_tool_grounding:
        grounding_block = (
            "Use tool calls to ground plans:\n"
            "1) search_places(query, location) to find real venues.\n"
            "2) get_directions(origin, destination, mode) for each member with known location."
        )
        if web_search_enabled:
            grounding_block += (
                "\n3) If search_places returns zero results, call web_search(query, location) "
                "to find alternatives."
            )
        logistics_example = (
            '"per_member": [\n'
            '  {"member": "...", "origin": "...", "duration": "...", "distance": "...", "mode": "..."}\n'
            "]"
        )
    else:
        grounding_block = (
            "Google Maps tools are unavailable in this environment. "
            "Do not mention missing tools or API keys. "
            "Generate realistic best-effort plans from member preferences, budgets, and recent events."
        )
        logistics_example = '"per_member": []'

    return f"""
Group name: {context['group']['name']}

Members:
{member_lines}

Recent events:
{history_lines}
{refinement_block}
{descriptor_block}
{lead_focus_block}
{prior_venues_block}
Generate exactly 5 plans with these vibe types in order: anchor, pivot, reach, chill, wildcard.
{novelty_block}
{grounding_block}

Return strict JSON with this schema:
{{
  "plans": [
    {{
      "title": "...",
      "description": "...",
      "vibe_type": "anchor|pivot|reach|chill|wildcard",
      "date_time": "ISO-8601 or null",
      "location": "...",
      "venue_name": "...",
      "estimated_cost": "...",
      "logistics": {{
        {logistics_example}
      }}
    }}
  ]
}}
""".strip()


async def _search_places(query: str, location: str, max_results: int = 3) -> dict[str, Any]:
    settings = get_settings()
    if not settings.google_maps_api_key:
        return {"error": "GOOGLE_MAPS_API_KEY not set"}

    safe_query = str(query or "").strip()
    safe_location = str(location or "").strip()
    if not safe_query:
        return {"error": "search_places query is required"}

    text_query = f"{safe_query} near {safe_location}" if safe_location else safe_query

    headers = {
        "X-Goog-Api-Key": settings.google_maps_api_key,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                PLACES_TEXT_SEARCH_URL,
                headers=headers,
                json={"textQuery": text_query},
            )
    except httpx.HTTPError as exc:
        return {
            "error": f"search_places failed: {exc.__class__.__name__}",
            "details": str(exc),
        }

    try:
        data = response.json()
    except ValueError:
        return {
            "error": f"search_places failed: non-JSON response (HTTP {response.status_code})",
            "details": response.text[:500],
        }

    if response.status_code >= 400:
        err = data.get("error") if isinstance(data, dict) else None
        message = err.get("message") if isinstance(err, dict) else response.text[:300]
        return {
            "error": f"search_places failed: HTTP {response.status_code}",
            "details": message,
        }

    api_error = data.get("error") if isinstance(data, dict) else None
    if isinstance(api_error, dict):
        return {
            "error": "search_places failed: upstream API error",
            "details": api_error.get("message") or str(api_error),
        }

    places = []
    raw_places = data.get("places") if isinstance(data, dict) else None
    for item in (raw_places or [])[: max(1, min(max_results, 10))]:
        display_name = item.get("displayName") if isinstance(item, dict) else None
        name = (
            display_name.get("text")
            if isinstance(display_name, dict)
            else item.get("name")
            if isinstance(item, dict)
            else None
        )
        address = item.get("formattedAddress") if isinstance(item, dict) else None
        places.append(
            {
                "name": name,
                "address": address,
                "rating": item.get("rating") if isinstance(item, dict) else None,
                "price_level": item.get("priceLevel") if isinstance(item, dict) else None,
            }
        )
    return {"places": places}


async def _web_search(
    query: str,
    location: str = "",
    max_results: int = 5,
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.tavily_api_key:
        return {"error": "TAVILY_API_KEY not set"}

    safe_query = str(query or "").strip()
    safe_location = str(location or "").strip()
    if not safe_query:
        return {"error": "web_search query is required"}

    combined_query = f"{safe_query} in {safe_location}" if safe_location else safe_query

    body: dict[str, Any] = {
        "query": combined_query,
        "search_depth": "basic",
        "max_results": max(1, min(max_results, 10)),
        "include_answer": False,
        "include_raw_content": False,
        "topic": "general",
    }

    headers = {
        "Authorization": f"Bearer {settings.tavily_api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                TAVILY_SEARCH_URL,
                headers=headers,
                json=body,
            )
    except httpx.HTTPError as exc:
        return {
            "error": f"web_search failed: {exc.__class__.__name__}",
            "details": str(exc),
        }

    try:
        data = response.json()
    except ValueError:
        return {
            "error": f"web_search failed: non-JSON response (HTTP {response.status_code})",
            "details": response.text[:500],
        }

    if response.status_code >= 400:
        message = ""
        if isinstance(data, dict):
            message = str(data.get("message") or data.get("error") or "")
        if not message:
            message = response.text[:300]
        return {
            "error": f"web_search failed: HTTP {response.status_code}",
            "details": message,
        }

    organic = data.get("results") if isinstance(data, dict) else None
    results: list[dict[str, Any]] = []
    for item in (organic or [])[: max(1, min(max_results, 10))]:
        if not isinstance(item, dict):
            continue
        link = str(item.get("url") or item.get("link") or "").strip()
        source = urlparse(link).netloc if link else ""
        results.append(
            {
                "title": str(item.get("title") or "").strip(),
                "link": link,
                "snippet": str(item.get("content") or item.get("snippet") or "").strip(),
                "source": source,
            }
        )

    return {"results": results}


async def _run_deterministic_web_fallback_search(
    context: dict[str, Any],
    refinement_notes: str | None = None,
    refinement_descriptors: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    base_location = _base_location_from_context(context)
    queries = _build_web_fallback_queries(
        context,
        refinement_notes=refinement_notes,
        refinement_descriptors=refinement_descriptors,
    )
    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    errors: list[str] = []

    for query in queries:
        payload = await _web_search(query=query, location=base_location, max_results=4)
        if payload.get("error"):
            details = str(payload.get("details") or payload["error"])
            errors.append(f"{payload['error']} ({details[:120]})")
            continue
        raw_results = payload.get("results")
        if not isinstance(raw_results, list):
            continue
        for item in raw_results:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            link = str(item.get("link") or "").strip()
            if not title and not link:
                continue
            key = (title.lower(), link.lower())
            if key in seen:
                continue
            seen.add(key)
            results.append(item)
            if len(results) >= 8:
                return results, errors

    return results, errors


async def _build_web_fallback_if_available(
    context: dict[str, Any],
    tool_messages: list[dict[str, Any]],
    reason: str,
    refinement_notes: str | None,
    enabled: bool,
    prior_venues: list[str] | None = None,
    novelty_target: float = 0.5,
    refinement_descriptors: list[str] | None = None,
) -> tuple[list[dict[str, Any]] | None, list[str]]:
    if not enabled:
        return None, []

    tool_summary = _summarize_tool_results(tool_messages)
    maps_empty = tool_summary["place_results"] == 0

    web_results = _extract_web_results_from_tool_messages(tool_messages)
    web_errors: list[str] = []
    if maps_empty and not web_results:
        web_results, web_errors = await _run_deterministic_web_fallback_search(
            context=context,
            refinement_notes=refinement_notes,
            refinement_descriptors=refinement_descriptors,
        )

    plans = _build_web_grounded_fallback_plans(
        context=context,
        web_results=web_results,
        reason=reason,
        refinement_notes=refinement_notes,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
    )
    return plans, web_errors


async def _synthesize_grounded_fallback_plans(
    context: dict[str, Any],
    tool_messages: list[dict[str, Any]],
    reason: str,
    refinement_notes: str | None,
    web_search_enabled: bool,
    prior_venues: list[str] | None = None,
    novelty_target: float = 0.5,
    refinement_descriptors: list[str] | None = None,
) -> tuple[list[dict[str, Any]] | None, list[str]]:
    synthesized = _build_maps_grounded_fallback_plans(
        context=context,
        tool_messages=tool_messages,
        reason=reason,
        refinement_notes=refinement_notes,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
    )
    if synthesized:
        return synthesized, []

    deterministic_places, map_errors = await _run_deterministic_maps_fallback_search(
        context=context,
        refinement_notes=refinement_notes,
        refinement_descriptors=refinement_descriptors,
    )
    synthesized = _build_maps_grounded_fallback_plans_from_places(
        context=context,
        places=deterministic_places,
        reason=reason,
        refinement_notes=refinement_notes,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
    )
    if synthesized:
        return synthesized, map_errors

    web_synthesized, web_errors = await _build_web_fallback_if_available(
        context=context,
        tool_messages=tool_messages,
        reason=reason,
        refinement_notes=refinement_notes,
        enabled=web_search_enabled,
        prior_venues=prior_venues,
        novelty_target=novelty_target,
        refinement_descriptors=refinement_descriptors,
    )
    if web_synthesized:
        return web_synthesized, [*map_errors, *web_errors]

    return None, [*map_errors, *web_errors]


async def _get_directions(
    origin: str, destination: str, mode: str = "driving"
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.google_maps_api_key:
        return {"error": "GOOGLE_MAPS_API_KEY not set"}

    safe_mode = mode if mode in {"driving", "transit", "walking"} else "driving"
    travel_mode = {
        "driving": "DRIVE",
        "walking": "WALK",
        "transit": "TRANSIT",
    }[safe_mode]

    body: dict[str, Any] = {
        "origin": {"address": origin},
        "destination": {"address": destination},
        "travelMode": travel_mode,
    }
    if travel_mode == "DRIVE":
        body["routingPreference"] = "TRAFFIC_AWARE"

    headers = {
        "X-Goog-Api-Key": settings.google_maps_api_key,
        "X-Goog-FieldMask": ROUTES_FIELD_MASK,
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                ROUTES_COMPUTE_URL,
                headers=headers,
                json=body,
            )
    except httpx.HTTPError as exc:
        return {
            "error": f"get_directions failed: {exc.__class__.__name__}",
            "details": str(exc),
        }

    try:
        data = response.json()
    except ValueError:
        return {
            "error": f"get_directions failed: non-JSON response (HTTP {response.status_code})",
            "details": response.text[:500],
        }

    if response.status_code >= 400:
        err = data.get("error") if isinstance(data, dict) else None
        message = err.get("message") if isinstance(err, dict) else response.text[:300]
        return {
            "error": f"get_directions failed: HTTP {response.status_code}",
            "details": message,
        }

    api_error = data.get("error") if isinstance(data, dict) else None
    if isinstance(api_error, dict):
        return {
            "error": "get_directions failed: upstream API error",
            "details": api_error.get("message") or str(api_error),
        }

    routes = data.get("routes") if isinstance(data, dict) else None
    if not routes:
        return {
            "error": "NO_ROUTE",
            "origin": origin,
            "destination": destination,
            "mode": safe_mode,
            "details": data,
        }

    route = routes[0]
    legs = route.get("legs") if isinstance(route, dict) else None
    leg = legs[0] if isinstance(legs, list) and legs else route
    distance_meters = leg.get("distanceMeters") if isinstance(leg, dict) else None
    if distance_meters is None and isinstance(route, dict):
        distance_meters = route.get("distanceMeters")
    duration_seconds = _duration_to_seconds(
        leg.get("duration") if isinstance(leg, dict) else None
    )
    if duration_seconds is None and isinstance(route, dict):
        duration_seconds = _duration_to_seconds(route.get("duration"))

    return {
        "origin": origin,
        "destination": destination,
        "distance": _format_distance(distance_meters),
        "duration": _format_duration(duration_seconds),
        "distance_meters": distance_meters,
        "duration_seconds": duration_seconds,
        "mode": safe_mode,
    }


async def _execute_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    logger.info("Planner invoking tool '%s'", name)
    if name == "search_places":
        result = await _search_places(**arguments)
    elif name == "get_directions":
        result = await _get_directions(**arguments)
    elif name == "web_search":
        result = await _web_search(**arguments)
    else:
        result = {"error": f"Unknown tool: {name}"}

    if isinstance(result, dict) and result.get("error"):
        logger.warning("Planner tool '%s' returned error: %s", name, result["error"])
    return result


async def _run_tool_loop(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    max_rounds: int = MAX_TOOL_ROUNDS,
) -> tuple[str, list[dict[str, Any]]]:
    work_messages = list(messages)
    consecutive_all_error_rounds = 0

    for _ in range(max_rounds):
        # Do not force tool_choice="auto": some OpenAI-compatible servers reject it.
        # Passing tools alone keeps behavior compatible across vLLM versions.
        response = await _call_vllm_chat(
            messages=work_messages,
            tools=tools,
            temperature=0.2,
            max_tokens=MAX_COMPLETION_TOKENS,
        )

        message = response.choices[0].message
        if not message.tool_calls:
            return message.content or "", work_messages

        work_messages.append(
            {
                "role": "assistant",
                "content": message.content or "",
                "tool_calls": [tc.model_dump(exclude_none=True) for tc in message.tool_calls],
            }
        )

        round_had_success = False
        for tool_call in message.tool_calls:
            tool_name = tool_call.function.name
            raw_args = tool_call.function.arguments or "{}"
            try:
                args = json.loads(raw_args)
                if not isinstance(args, dict):
                    args = {}
            except json.JSONDecodeError:
                args = {}

            try:
                tool_result = await _execute_tool(tool_name, args)
            except Exception as exc:  # pragma: no cover - guardrail
                tool_result = {
                    "error": f"Tool execution failed: {exc.__class__.__name__}",
                    "details": str(exc),
                }

            if not (isinstance(tool_result, dict) and tool_result.get("error")):
                round_had_success = True

            work_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(tool_result),
                }
            )

        if round_had_success:
            consecutive_all_error_rounds = 0
        else:
            consecutive_all_error_rounds += 1
            if consecutive_all_error_rounds >= 2:
                logger.warning(
                    "Planner stopping tool loop early after %s consecutive all-error rounds.",
                    consecutive_all_error_rounds,
                )
                summary = _summarize_tool_results(work_messages)
                if summary["place_results"] > 0 or summary["web_results"] > 0:
                    # Avoid another expensive generation step when we already have venue grounding.
                    return '{"plans":[]}', work_messages
                break

    summary = _summarize_tool_results(work_messages)
    if summary["place_results"] > 0 or summary["web_results"] > 0:
        logger.warning(
            "Planner collected grounded tool candidates (places=%s web=%s); "
            "skipping extra LLM finalize call.",
            summary["place_results"],
            summary["web_results"],
        )
        # Let caller synthesize deterministic grounded plans.
        return '{"plans":[]}', work_messages

    work_messages.append(
        {
            "role": "user",
            "content": "Finalize now and return valid JSON with exactly 5 plans.",
        }
    )
    final = await _call_vllm_chat(
        messages=work_messages,
        temperature=0.2,
        max_tokens=MAX_COMPLETION_TOKENS,
    )
    return final.choices[0].message.content or "", work_messages


async def _run_structured_retry(
    prompt: str,
    prior_output: str,
    system_prompt: str,
) -> str:
    """Ask the model to rewrite prior output into strict schema-valid JSON."""
    response = await _call_vllm_chat(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
            {
                "role": "assistant",
                "content": prior_output,
            },
            {
                "role": "user",
                "content": (
                    "Your last response was not parser-safe. "
                    "Return ONLY valid minified JSON with key 'plans' and exactly 5 plan objects "
                    "matching the required schema. "
                    "Do not include markdown fences, explanations, comments, or any text outside JSON. "
                    "Start with '{' and end with '}'."
                ),
            },
        ],
        temperature=0.0,
        max_tokens=REPAIR_MAX_COMPLETION_TOKENS,
    )
    return response.choices[0].message.content or ""


async def generate_group_plans(
    group_id: UUID,
    refinement_notes: str | None = None,
    planning_constraints: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Generate and normalize 5 plans for a group using vLLM tool-calling."""
    context = await _load_group_context(group_id)
    settings = get_settings()
    constraints = planning_constraints if isinstance(planning_constraints, dict) else {}
    novelty_target = _clamp_novelty_target(
        constraints.get("novelty_target"),
        default=0.35 if refinement_notes else 0.7,
    )
    prior_venues = [
        str(venue).strip()
        for venue in (constraints.get("prior_venues") or [])
        if str(venue).strip()
    ]
    refinement_descriptors = [
        str(descriptor).strip().lower()
        for descriptor in (constraints.get("refinement_descriptors") or [])
        if str(descriptor).strip()
    ]
    refinement_focus_note = str(constraints.get("refinement_focus_note") or "").strip()
    use_tool_grounding = bool(settings.google_maps_api_key.strip())
    use_web_search = bool(settings.tavily_api_key.strip())
    tools = (
        PLANNER_TOOLS
        if use_web_search
        else [tool for tool in PLANNER_TOOLS if tool["function"]["name"] != "web_search"]
    )
    system_prompt = (
        SYSTEM_PROMPT_TOOL_GROUNDED if use_tool_grounding else SYSTEM_PROMPT_BEST_EFFORT
    )
    prompt = _build_prompt(
        context,
        refinement_notes=refinement_notes,
        require_tool_grounding=use_tool_grounding,
        web_search_enabled=use_web_search,
        novelty_target=novelty_target,
        prior_venues=prior_venues,
        refinement_descriptors=refinement_descriptors,
        refinement_focus_note=refinement_focus_note,
    )

    try:
        tool_messages: list[dict[str, Any]] = []
        if use_tool_grounding:
            logger.warning(
                "GOOGLE_MAPS_API_KEY detected; generating tool-grounded plans for group %s "
                "(web_search=%s).",
                group_id,
                "enabled" if use_web_search else "disabled",
            )
            try:
                output, tool_messages = await _run_tool_loop(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    tools=tools,
                )
            except Exception as tool_exc:
                logger.warning(
                    "Planner tool loop failed for group %s (%s: %s); "
                    "attempting deterministic grounded fallback.",
                    group_id,
                    tool_exc.__class__.__name__,
                    tool_exc,
                )
                synthesized, grounded_errors = await _synthesize_grounded_fallback_plans(
                    context=context,
                    tool_messages=tool_messages,
                    reason=f"Tool loop failed: {tool_exc.__class__.__name__}: {tool_exc}",
                    refinement_notes=refinement_notes,
                    web_search_enabled=use_web_search,
                    prior_venues=prior_venues,
                    novelty_target=novelty_target,
                    refinement_descriptors=refinement_descriptors,
                )
                if synthesized:
                    logger.warning(
                        "Planner tool loop failed for group %s; using deterministic grounded fallback.",
                        group_id,
                    )
                    return synthesized
                details = " | ".join(grounded_errors[:2]) if grounded_errors else str(tool_exc)
                raise PlannerError(
                    f"Tool-grounded planning failed before model output ({details})"
                ) from tool_exc

            tool_summary = _summarize_tool_results(tool_messages)
            logger.warning(
                "Planner tool summary for group %s: calls=%s place_calls=%s place_results=%s "
                "web_calls=%s web_results=%s errors=%s",
                group_id,
                tool_summary["tool_calls"],
                tool_summary["place_calls"],
                tool_summary["place_results"],
                tool_summary["web_calls"],
                tool_summary["web_results"],
                len(tool_summary["errors"]),
            )
        else:
            logger.warning(
                "GOOGLE_MAPS_API_KEY missing; generating best-effort plans without tool grounding."
            )
            response = await _call_vllm_chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=MAX_COMPLETION_TOKENS,
            )
            output = response.choices[0].message.content or ""
        try:
            return _extract_plans(output)
        except PlannerError as parse_exc:
            parse_message = str(parse_exc)
            if use_tool_grounding and "no plans" in parse_message.lower():
                tool_summary = _summarize_tool_results(tool_messages)
                synthesized, grounded_errors = await _synthesize_grounded_fallback_plans(
                    context=context,
                    tool_messages=tool_messages,
                    reason=f"LLM produced empty plans: {parse_message}",
                    refinement_notes=refinement_notes,
                    web_search_enabled=use_web_search,
                    prior_venues=prior_venues,
                    novelty_target=novelty_target,
                    refinement_descriptors=refinement_descriptors,
                )
                if synthesized:
                    logger.warning(
                        "Planner returned no plans for group %s; using deterministic grounded fallback synthesis.",
                        group_id,
                    )
                    return synthesized
                details_parts: list[str] = []
                if tool_summary["errors"]:
                    details_parts.append("; ".join(tool_summary["errors"][:2]))
                if grounded_errors:
                    details_parts.append("; ".join(grounded_errors[:2]))
                details = " | ".join(details_parts) or "no usable grounded venues were returned"
                raise PlannerError(
                    f"LLM returned no plans and map search produced no usable venues ({details})"
                ) from parse_exc

            if use_tool_grounding:
                tool_summary = _summarize_tool_results(tool_messages)
                if tool_summary["place_results"] > 0 or tool_summary["web_results"] > 0:
                    synthesized, _ = await _synthesize_grounded_fallback_plans(
                        context=context,
                        tool_messages=tool_messages,
                        reason=f"LLM parse failed: {parse_message}",
                        refinement_notes=refinement_notes,
                        web_search_enabled=use_web_search,
                        prior_venues=prior_venues,
                        novelty_target=novelty_target,
                        refinement_descriptors=refinement_descriptors,
                    )
                    if synthesized:
                        logger.warning(
                            "Planner parse failed for group %s; using deterministic grounded fallback synthesis.",
                            group_id,
                        )
                        return synthesized

            snippet = _strip_code_fence(output)[:800].replace("\n", "\\n")
            logger.warning(
                "Planner parse failed after tool loop for group %s: %s. "
                "Output length=%d snippet=%s Retrying structured output.",
                group_id,
                parse_exc,
                len(output or ""),
                snippet,
            )
            repaired = await _run_structured_retry(
                prompt=prompt,
                prior_output=output,
                system_prompt=system_prompt,
            )
            repaired_snippet = _strip_code_fence(repaired)[:800].replace("\n", "\\n")
            logger.info(
                "Planner structured retry output for group %s: length=%d snippet=%s",
                group_id,
                len(repaired or ""),
                repaired_snippet,
            )
            try:
                return _extract_plans(repaired)
            except PlannerError as repaired_exc:
                if use_tool_grounding:
                    synthesized, _ = await _synthesize_grounded_fallback_plans(
                        context=context,
                        tool_messages=tool_messages,
                        reason=f"Structured retry failed: {repaired_exc}",
                        refinement_notes=refinement_notes,
                        web_search_enabled=use_web_search,
                        prior_venues=prior_venues,
                        novelty_target=novelty_target,
                        refinement_descriptors=refinement_descriptors,
                    )
                    if synthesized:
                        logger.warning(
                            "Structured retry failed for group %s; using deterministic grounded fallback synthesis.",
                            group_id,
                        )
                        return synthesized
                raise
    except Exception as exc:
        if settings.planner_fallback_enabled:
            reason = f"{exc.__class__.__name__}: {str(exc)}"
            return _build_fallback_plans(
                context=context,
                reason=reason,
                refinement_notes=refinement_notes,
            )
        raise PlannerError(f"Planner failed: {exc.__class__.__name__}: {exc}") from exc

"""Materialization jobs that build planner-facing analytics tables."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from typing import Any
from uuid import UUID

from database import db

DESCRIPTORS = (
    "budget_friendly",
    "short_travel",
    "more_active",
    "more_chill",
    "indoor",
    "outdoor",
    "food_focused",
    "accessible",
)
FEATURE_VERSION = "analytics-v1"


@dataclass
class GroupSignals:
    top_activity_tags: list[str]
    budget_mode: str
    mobility_mode: str
    historical_novelty_score: float | None
    refine_descriptor_weights: dict[str, float]


def _normalize_token(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    return " ".join(token.split())


def _mode(values: list[str], fallback: str) -> str:
    normalized = [_normalize_token(v) for v in values if _normalize_token(v)]
    if not normalized:
        return fallback
    return Counter(normalized).most_common(1)[0][0]


def _descriptor_weights(
    *,
    top_tags: list[str],
    budget_mode: str,
    mobility_mode: str,
    historical_novelty: float | None,
    winning_vibes: dict[str, int],
) -> dict[str, float]:
    weights = {name: 0.2 for name in DESCRIPTORS}

    if any(token in budget_mode for token in ("budget", "low", "cheap", "student")):
        weights["budget_friendly"] += 0.45
    elif any(token in budget_mode for token in ("high", "luxury", "premium")):
        weights["budget_friendly"] -= 0.1

    if mobility_mode == "local":
        weights["short_travel"] += 0.35
    elif mobility_mode == "mixed":
        weights["short_travel"] += 0.2
    elif mobility_mode == "distributed":
        weights["short_travel"] += 0.05
        weights["accessible"] += 0.2

    tag_blob = " ".join(top_tags)
    if any(token in tag_blob for token in ("hike", "park", "outdoor", "beach", "trail")):
        weights["outdoor"] += 0.4
    if any(token in tag_blob for token in ("board game", "cafe", "movie", "museum", "indoor")):
        weights["indoor"] += 0.35
    if any(token in tag_blob for token in ("restaurant", "dinner", "food", "brunch", "cafe")):
        weights["food_focused"] += 0.35
    if any(token in tag_blob for token in ("run", "sport", "climb", "dance", "activity")):
        weights["more_active"] += 0.3
    if any(token in tag_blob for token in ("chill", "conversation", "relax", "quiet")):
        weights["more_chill"] += 0.3
    if any(token in tag_blob for token in ("wheelchair", "accessible", "transit", "easy")):
        weights["accessible"] += 0.3

    total_wins = sum(winning_vibes.values())
    if total_wins > 0:
        active_wins = winning_vibes.get("reach", 0) + winning_vibes.get("wildcard", 0)
        chill_wins = winning_vibes.get("anchor", 0) + winning_vibes.get("chill", 0)
        weights["more_active"] += 0.4 * (active_wins / total_wins)
        weights["more_chill"] += 0.4 * (chill_wins / total_wins)
        weights["food_focused"] += 0.25 * (winning_vibes.get("pivot", 0) / total_wins)

    if historical_novelty is not None:
        if historical_novelty < 0.45:
            weights["more_active"] += 0.2
            weights["outdoor"] += 0.15
        elif historical_novelty > 0.75:
            weights["more_chill"] += 0.1
            weights["budget_friendly"] += 0.05

    return {key: max(0.0, min(1.0, round(value, 3))) for key, value in weights.items()}


async def build_plan_outcome_facts() -> int:
    """Refresh plan-level outcome facts from product tables."""
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE analytics.plan_outcome_fact")
            row = await conn.fetchrow(
                """
                WITH ranked_votes AS (
                    SELECT
                        v.plan_round_id,
                        rv.plan_id::uuid AS plan_id,
                        rv.rank_position::integer AS rank_position
                    FROM votes v
                    CROSS JOIN LATERAL jsonb_array_elements_text(v.rankings) WITH ORDINALITY
                        AS rv(plan_id, rank_position)
                ),
                vote_agg AS (
                    SELECT
                        plan_round_id,
                        plan_id,
                        AVG(rank_position)::numeric(6,3) AS avg_vote_rank
                    FROM ranked_votes
                    GROUP BY plan_round_id, plan_id
                ),
                feedback_agg AS (
                    SELECT
                        e.plan_id,
                        AVG(CASE WHEN f.attended THEN 1.0 ELSE 0.0 END)::numeric(5,4) AS attended_rate,
                        AVG(CASE WHEN f.rating = 'loved' THEN 1.0 ELSE 0.0 END)::numeric(5,4) AS feedback_loved_rate
                    FROM events e
                    LEFT JOIN feedback f ON f.event_id = e.id
                    GROUP BY e.plan_id
                ),
                inserted AS (
                    INSERT INTO analytics.plan_outcome_fact (
                        plan_id,
                        group_id,
                        plan_round_id,
                        iteration,
                        won,
                        avg_vote_rank,
                        attended_rate,
                        feedback_loved_rate,
                        cost_bucket,
                        vibe_type,
                        venue_key,
                        created_at
                    )
                    SELECT
                        p.id,
                        pr.group_id,
                        p.plan_round_id,
                        pr.iteration,
                        COALESCE(pr.winning_plan_id = p.id, FALSE) AS won,
                        va.avg_vote_rank,
                        COALESCE(fa.attended_rate, 0.0),
                        COALESCE(fa.feedback_loved_rate, 0.0),
                        p.estimated_cost,
                        p.vibe_type,
                        lower(trim(COALESCE(NULLIF(p.venue_name, ''), NULLIF(p.title, '')))) AS venue_key,
                        p.created_at
                    FROM plans p
                    JOIN plan_rounds pr ON pr.id = p.plan_round_id
                    LEFT JOIN vote_agg va
                        ON va.plan_round_id = p.plan_round_id
                       AND va.plan_id = p.id
                    LEFT JOIN feedback_agg fa ON fa.plan_id = p.id
                    RETURNING 1
                )
                SELECT COUNT(*)::integer AS inserted_count
                FROM inserted
                """,
            )
    return int(row["inserted_count"] if row else 0)


async def build_venue_performance_priors() -> int:
    """Refresh venue-level priors from plan outcomes."""
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE analytics.venue_performance_prior")
            row = await conn.fetchrow(
                """
                WITH inserted AS (
                    INSERT INTO analytics.venue_performance_prior (
                        group_id,
                        venue_key,
                        win_rate,
                        avg_rank,
                        attendance_rate,
                        feedback_score,
                        sample_size,
                        updated_at
                    )
                    SELECT
                        group_id,
                        venue_key,
                        AVG(CASE WHEN won THEN 1.0 ELSE 0.0 END)::numeric(5,4) AS win_rate,
                        AVG(avg_vote_rank)::numeric(6,3) AS avg_rank,
                        AVG(attended_rate)::numeric(5,4) AS attendance_rate,
                        AVG(feedback_loved_rate)::numeric(5,4) AS feedback_score,
                        COUNT(*)::integer AS sample_size,
                        NOW() AS updated_at
                    FROM analytics.plan_outcome_fact
                    WHERE venue_key IS NOT NULL
                      AND venue_key <> ''
                    GROUP BY group_id, venue_key
                    RETURNING 1
                )
                SELECT COUNT(*)::integer AS inserted_count
                FROM inserted
                """,
            )
    return int(row["inserted_count"] if row else 0)


async def _build_group_signals(group_id: UUID, conn) -> GroupSignals:
    pref_rows = await conn.fetch(
        """
        SELECT activity_likes, budget_preference, default_location
        FROM group_preferences
        WHERE group_id = $1
        """,
        group_id,
    )
    tags_counter: Counter[str] = Counter()
    budget_values: list[str] = []
    locations: set[str] = set()

    for row in pref_rows:
        likes = row["activity_likes"]
        if isinstance(likes, str):
            try:
                likes = json.loads(likes)
            except json.JSONDecodeError:
                likes = []
        if isinstance(likes, list):
            for raw in likes:
                token = _normalize_token(raw)
                if token:
                    tags_counter[token] += 1
        budget = _normalize_token(row["budget_preference"])
        if budget:
            budget_values.append(budget)
        location = _normalize_token(row["default_location"])
        if location:
            locations.add(location)

    top_tags = [tag for tag, _ in tags_counter.most_common(5)]
    budget_mode = _mode(budget_values, fallback="unspecified")
    if len(locations) <= 1:
        mobility_mode = "local" if locations else "unknown"
    elif len(locations) == 2:
        mobility_mode = "mixed"
    else:
        mobility_mode = "distributed"

    novelty_row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE venue_key IS NOT NULL AND venue_key <> '')::integer AS total_rows,
            COUNT(DISTINCT venue_key) FILTER (WHERE venue_key IS NOT NULL AND venue_key <> '')::integer AS distinct_rows
        FROM analytics.plan_outcome_fact
        WHERE group_id = $1
        """,
        group_id,
    )
    historical_novelty = None
    if novelty_row and novelty_row["total_rows"]:
        historical_novelty = round(
            float(novelty_row["distinct_rows"]) / float(novelty_row["total_rows"]),
            4,
        )

    winning_rows = await conn.fetch(
        """
        SELECT vibe_type, COUNT(*)::integer AS wins
        FROM analytics.plan_outcome_fact
        WHERE group_id = $1
          AND won = TRUE
        GROUP BY vibe_type
        """,
        group_id,
    )
    winning_vibes = {
        _normalize_token(row["vibe_type"]): int(row["wins"])
        for row in winning_rows
        if _normalize_token(row["vibe_type"])
    }

    return GroupSignals(
        top_activity_tags=top_tags,
        budget_mode=budget_mode,
        mobility_mode=mobility_mode,
        historical_novelty_score=historical_novelty,
        refine_descriptor_weights=_descriptor_weights(
            top_tags=top_tags,
            budget_mode=budget_mode,
            mobility_mode=mobility_mode,
            historical_novelty=historical_novelty,
            winning_vibes=winning_vibes,
        ),
    )


async def build_group_feature_snapshots(feature_version: str = FEATURE_VERSION) -> int:
    """Create a latest feature snapshot row for each group."""
    group_rows = await db.fetch("SELECT id FROM groups WHERE status = 'active'")
    inserted_count = 0

    async with db.acquire() as conn:
        async with conn.transaction():
            for row in group_rows:
                group_id = row["id"]
                signals = await _build_group_signals(group_id, conn)
                await conn.execute(
                    """
                    UPDATE analytics.group_feature_snapshot
                    SET is_latest = FALSE
                    WHERE group_id = $1
                      AND is_latest = TRUE
                    """,
                    group_id,
                )
                await conn.execute(
                    """
                    INSERT INTO analytics.group_feature_snapshot (
                        group_id,
                        snapshot_at,
                        feature_version,
                        top_activity_tags,
                        budget_mode,
                        mobility_mode,
                        historical_novelty_score,
                        refine_descriptor_weights,
                        is_latest
                    )
                    VALUES ($1, NOW(), $2, $3::text[], $4, $5, $6, $7::jsonb, TRUE)
                    """,
                    group_id,
                    feature_version,
                    signals.top_activity_tags,
                    signals.budget_mode,
                    signals.mobility_mode,
                    signals.historical_novelty_score,
                    json.dumps(signals.refine_descriptor_weights),
                )
                inserted_count += 1

    return inserted_count

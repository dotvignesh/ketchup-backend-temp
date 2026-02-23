"""Plan round service (generation, voting, refinement, finalization)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from uuid import UUID

from agents.planning import PlannerError, generate_group_plans
from config import get_settings
from database import db
from services.errors import BadRequestError, NotFoundError, UpstreamServiceError
from services.group_access import require_active_group_member, require_group_lead

VOTING_WINDOW_HOURS = 24
DEFAULT_EVENT_OFFSET_DAYS = 7
RECENT_VENUE_LIMIT = 40

REFINEMENT_DESCRIPTOR_GUIDANCE: dict[str, str] = {
    "budget_friendly": "Prefer lower cost and better value options.",
    "short_travel": "Minimize travel time and distance for most members.",
    "more_active": "Bias toward higher-energy, activity-heavy options.",
    "more_chill": "Bias toward calmer, conversation-friendly options.",
    "indoor": "Prefer indoor or weather-safe venues.",
    "outdoor": "Prefer outdoor options when feasible.",
    "food_focused": "Favor food-centric experiences.",
    "accessible": "Prioritize easy-access, low-friction logistics.",
}


def _parse_rankings(raw_rankings: str | list[str] | None) -> list[str]:
    if not raw_rankings:
        return []
    if isinstance(raw_rankings, str):
        try:
            parsed = json.loads(raw_rankings)
        except json.JSONDecodeError:
            return []
        return [str(value) for value in parsed if value]
    return [str(value) for value in raw_rankings if value]


def _first_choice_counts(votes: list) -> dict[str, int]:
    counts: dict[str, int] = {}
    for vote in votes:
        rankings = _parse_rankings(vote["rankings"])
        if rankings:
            first_choice = rankings[0]
            counts[first_choice] = counts.get(first_choice, 0) + 1
    return counts


def _clamp_novelty_target(value: float, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, min(1.0, parsed))


def _normalize_refinement_descriptors(descriptors: list[str] | None) -> list[str]:
    if not descriptors:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in descriptors:
        token = str(raw or "").strip().lower()
        if not token or token in seen or token not in REFINEMENT_DESCRIPTOR_GUIDANCE:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def _descriptor_guidance(descriptors: list[str]) -> list[str]:
    return [REFINEMENT_DESCRIPTOR_GUIDANCE[d] for d in descriptors if d in REFINEMENT_DESCRIPTOR_GUIDANCE]


def _build_refinement_notes(
    votes: list,
    descriptors: list[str] | None = None,
    lead_note: str | None = None,
) -> str:
    first_choice_counts = _first_choice_counts(votes)
    notes: list[str] = []
    for vote in votes:
        if vote["notes"]:
            notes.append(str(vote["notes"]))
    normalized_descriptors = _normalize_refinement_descriptors(descriptors)
    return json.dumps(
        {
            "first_choice_counts": first_choice_counts,
            "notes": notes[:10],
            "descriptors": normalized_descriptors,
            "descriptor_guidance": _descriptor_guidance(normalized_descriptors),
            "lead_note": (lead_note or "").strip(),
        }
    )


async def _fetch_recent_venue_names(group_id: UUID, limit: int = RECENT_VENUE_LIMIT) -> list[str]:
    rows = await db.fetch(
        """
        SELECT COALESCE(NULLIF(TRIM(p.venue_name), ''), NULLIF(TRIM(p.title), '')) AS venue
        FROM plans p
        JOIN plan_rounds pr ON pr.id = p.plan_round_id
        WHERE pr.group_id = $1
        ORDER BY p.created_at DESC
        LIMIT $2
        """,
        group_id,
        limit,
    )
    venues: list[str] = []
    seen: set[str] = set()
    for row in rows:
        venue = row["venue"]
        if not venue:
            continue
        venue_s = str(venue).strip()
        if not venue_s:
            continue
        key = venue_s.lower()
        if key in seen:
            continue
        seen.add(key)
        venues.append(venue_s)
    return venues


async def _insert_generated_plans(
    round_id: UUID, plans: list[dict]
) -> list[dict[str, str | None]]:
    saved: list[dict[str, str | None]] = []
    for plan in plans:
        row = await db.fetchrow(
            """
            INSERT INTO plans
                (plan_round_id, title, description, vibe_type, date_time, location, venue_name, estimated_cost, logistics)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, title, description, vibe_type, date_time, location, venue_name, estimated_cost
            """,
            round_id,
            plan.get("title"),
            plan.get("description"),
            plan.get("vibe_type"),
            plan.get("date_time"),
            plan.get("location"),
            plan.get("venue_name"),
            plan.get("estimated_cost"),
            json.dumps(plan.get("logistics") or {}),
        )
        saved.append(
            {
                "id": str(row["id"]),
                "title": row["title"],
                "description": row["description"],
                "vibe_type": row["vibe_type"],
                "location": row["location"],
                "venue_name": row["venue_name"],
                "estimated_cost": row["estimated_cost"],
            }
        )
    return saved


async def _create_generation_round(group_id: UUID) -> tuple[UUID, int, datetime]:
    max_iter = await db.fetchval(
        "SELECT COALESCE(MAX(iteration), 0) FROM plan_rounds WHERE group_id = $1",
        group_id,
    )
    iteration = (max_iter or 0) + 1
    voting_deadline = datetime.utcnow() + timedelta(hours=VOTING_WINDOW_HOURS)
    round_row = await db.fetchrow(
        """
        INSERT INTO plan_rounds (group_id, iteration, status, voting_deadline)
        VALUES ($1, $2, 'generating', $3)
        RETURNING id
        """,
        group_id,
        iteration,
        voting_deadline,
    )
    return round_row["id"], iteration, voting_deadline


async def generate_plans(group_id: UUID, user_id: UUID) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)
    await require_group_lead(
        group_id,
        user_id,
        detail="Only group lead can generate plans",
    )

    round_id, _, voting_deadline = await _create_generation_round(group_id)
    settings = get_settings()
    novelty_target = _clamp_novelty_target(
        settings.planner_novelty_target_generate,
        default=0.7,
    )
    prior_venues = await _fetch_recent_venue_names(group_id)

    try:
        generated = await generate_group_plans(
            group_id,
            planning_constraints={
                "plan_mode": "generate",
                "novelty_target": novelty_target,
                "prior_venues": prior_venues,
            },
        )
        plans_data = await _insert_generated_plans(round_id, generated)
        await db.execute("UPDATE plan_rounds SET status = 'voting_open' WHERE id = $1", round_id)
    except PlannerError as exc:
        await db.execute(
            "UPDATE plan_rounds SET status = 'manual_handoff' WHERE id = $1",
            round_id,
        )
        raise UpstreamServiceError(f"Plan generation failed: {exc}") from exc
    except Exception as exc:
        await db.execute(
            "UPDATE plan_rounds SET status = 'manual_handoff' WHERE id = $1",
            round_id,
        )
        raise UpstreamServiceError("Plan generation failed") from exc

    return {
        "plan_round_id": str(round_id),
        "plans": plans_data,
        "status": "voting_open",
        "voting_deadline": voting_deadline.isoformat(),
    }


async def get_plans(group_id: UUID, round_id: UUID, user_id: UUID) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    round_row = await db.fetchrow(
        "SELECT id, voting_deadline FROM plan_rounds WHERE id = $1 AND group_id = $2",
        round_id,
        group_id,
    )
    if not round_row:
        raise NotFoundError("Plan round not found")

    plans = await db.fetch(
        "SELECT id, title, description, vibe_type, date_time, location, venue_name, estimated_cost, logistics FROM plans WHERE plan_round_id = $1 ORDER BY vibe_type",
        round_id,
    )

    return {
        "plans": [
            {
                "id": str(p["id"]),
                "title": p["title"],
                "description": p["description"],
                "vibe_type": p["vibe_type"],
                "date_time": p["date_time"].isoformat() if p["date_time"] else None,
                "location": p["location"],
                "venue_name": p["venue_name"],
                "estimated_cost": p["estimated_cost"],
                "logistics": p["logistics"] or {},
            }
            for p in plans
        ],
        "voting_deadline": round_row["voting_deadline"].isoformat()
        if round_row["voting_deadline"]
        else None,
        "user_logistics": {},
    }


async def submit_vote(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID,
    rankings: list[UUID],
    notes: str | None,
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    round_row = await db.fetchrow(
        "SELECT id FROM plan_rounds WHERE id = $1 AND group_id = $2 AND status = 'voting_open'",
        round_id,
        group_id,
    )
    if not round_row:
        raise NotFoundError("Plan round not found or voting closed")

    plans_in_round = await db.fetch(
        "SELECT id FROM plans WHERE plan_round_id = $1",
        round_id,
    )
    plan_ids = {str(plan["id"]) for plan in plans_in_round}
    for plan_id in rankings:
        if str(plan_id) not in plan_ids:
            raise BadRequestError(f"Invalid plan id: {plan_id}")

    serialized_rankings = [str(ranking) for ranking in rankings]
    await db.execute(
        """
        INSERT INTO votes (plan_round_id, user_id, rankings, notes)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (plan_round_id, user_id) DO UPDATE SET rankings = EXCLUDED.rankings, notes = EXCLUDED.notes
        """,
        round_id,
        user_id,
        json.dumps(serialized_rankings),
        notes,
    )

    return {
        "vote_id": "ok",
        "rankings": serialized_rankings,
        "notes": notes,
    }


async def get_voting_results(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID,
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    round_row = await db.fetchrow(
        "SELECT id FROM plan_rounds WHERE id = $1 AND group_id = $2",
        round_id,
        group_id,
    )
    if not round_row:
        raise NotFoundError("Plan round not found")

    votes = await db.fetch(
        "SELECT rankings FROM votes WHERE plan_round_id = $1",
        round_id,
    )
    first_choices = _first_choice_counts(votes)

    total_members = await db.fetchval(
        "SELECT COUNT(*) FROM group_members WHERE group_id = $1 AND status = 'active'",
        group_id,
    )

    consensus = False
    winning_plan_id = None
    if first_choices and total_members:
        max_votes = max(first_choices.values())
        if max_votes >= (total_members / 2) + 1:
            winning_plan_id = max(first_choices, key=first_choices.get)
            consensus = True

    return {
        "consensus": consensus,
        "winning_plan_id": winning_plan_id,
        "vote_summary": first_choices,
        "iteration_count": 1,
    }


async def refine_plans(
    group_id: UUID,
    round_id: UUID,
    user_id: UUID,
    descriptors: list[str] | None = None,
    lead_note: str | None = None,
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)
    await require_group_lead(group_id, user_id, detail="Only group lead can refine")

    current_round = await db.fetchrow(
        "SELECT id FROM plan_rounds WHERE id = $1 AND group_id = $2",
        round_id,
        group_id,
    )
    if not current_round:
        raise NotFoundError("Plan round not found")

    await db.execute(
        "UPDATE plan_rounds SET status = 'manual_handoff' WHERE id = $1",
        round_id,
    )

    new_round_id, iteration, voting_deadline = await _create_generation_round(group_id)
    vote_rows = await db.fetch(
        "SELECT rankings, notes FROM votes WHERE plan_round_id = $1",
        round_id,
    )
    normalized_descriptors = _normalize_refinement_descriptors(descriptors)
    refinement_notes = _build_refinement_notes(
        vote_rows,
        descriptors=normalized_descriptors,
        lead_note=lead_note,
    )
    settings = get_settings()
    novelty_target = _clamp_novelty_target(
        settings.planner_novelty_target_refine,
        default=0.35,
    )
    prior_venues = await _fetch_recent_venue_names(group_id)

    try:
        generated = await generate_group_plans(
            group_id,
            refinement_notes=refinement_notes,
            planning_constraints={
                "plan_mode": "refine",
                "novelty_target": novelty_target,
                "prior_venues": prior_venues,
                "refinement_descriptors": normalized_descriptors,
                "refinement_focus_note": (lead_note or "").strip(),
            },
        )
        plans_data = await _insert_generated_plans(new_round_id, generated)
        await db.execute(
            "UPDATE plan_rounds SET status = 'voting_open' WHERE id = $1",
            new_round_id,
        )
    except PlannerError as exc:
        await db.execute(
            "UPDATE plan_rounds SET status = 'manual_handoff' WHERE id = $1",
            new_round_id,
        )
        raise UpstreamServiceError(f"Plan refinement failed: {exc}") from exc
    except Exception as exc:
        await db.execute(
            "UPDATE plan_rounds SET status = 'manual_handoff' WHERE id = $1",
            new_round_id,
        )
        raise UpstreamServiceError("Plan refinement failed") from exc

    return {
        "plan_round_id": str(new_round_id),
        "plans": plans_data,
        "status": "voting_open",
        "iteration": iteration,
        "voting_deadline": voting_deadline.isoformat(),
    }


async def finalize_plan(group_id: UUID, round_id: UUID, user_id: UUID) -> dict[str, str]:
    await require_group_lead(group_id, user_id, detail="Only group lead can finalize")

    round_row = await db.fetchrow(
        "SELECT id, winning_plan_id FROM plan_rounds WHERE id = $1 AND group_id = $2",
        round_id,
        group_id,
    )
    if not round_row:
        raise NotFoundError("Plan round not found")

    winning_id = round_row["winning_plan_id"]
    if not winning_id:
        votes = await db.fetch("SELECT rankings FROM votes WHERE plan_round_id = $1", round_id)
        first_choices = _first_choice_counts(votes)
        if first_choices:
            winning_id = UUID(max(first_choices, key=first_choices.get))
            await db.execute(
                "UPDATE plan_rounds SET winning_plan_id = $1 WHERE id = $2",
                winning_id,
                round_id,
            )

    if not winning_id:
        raise BadRequestError("Cannot finalize without a winning plan")

    plan = await db.fetchrow(
        "SELECT id, title, date_time FROM plans WHERE id = $1",
        winning_id,
    )
    if not plan:
        raise NotFoundError("Winning plan not found")

    event_date = plan["date_time"] or datetime.utcnow() + timedelta(
        days=DEFAULT_EVENT_OFFSET_DAYS
    )
    event_row = await db.fetchrow(
        """
        INSERT INTO events (group_id, plan_id, plan_round_id, event_date)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (plan_round_id) DO UPDATE SET
            plan_id = EXCLUDED.plan_id,
            event_date = EXCLUDED.event_date
        RETURNING id, event_date
        """,
        group_id,
        winning_id,
        round_id,
        event_date,
    )
    await db.execute(
        "UPDATE plan_rounds SET status = 'consensus_reached', winning_plan_id = $1 WHERE id = $2",
        winning_id,
        round_id,
    )

    return {
        "event_id": str(event_row["id"]),
        "plan_title": plan["title"],
        "event_date": event_row["event_date"].isoformat(),
    }

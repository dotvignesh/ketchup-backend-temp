"""Seed deterministic mock source data so analytics DAGs can process real rows."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from analytics.bootstrap import ensure_analytics_schema
from database import db


def _stable_uuid(key: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"ketchup-mock::{key}")


async def _upsert_users(conn) -> list[UUID]:
    users = [
        ("alice.mock@ketchup.local", "Alice Mock"),
        ("bob.mock@ketchup.local", "Bob Mock"),
        ("carla.mock@ketchup.local", "Carla Mock"),
        ("diego.mock@ketchup.local", "Diego Mock"),
        ("eva.mock@ketchup.local", "Eva Mock"),
        ("farah.mock@ketchup.local", "Farah Mock"),
    ]
    ids: list[UUID] = []
    for email, name in users:
        user_id = _stable_uuid(f"user:{email}")
        ids.append(user_id)
        await conn.execute(
            """
            INSERT INTO users (id, email, name, google_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id)
            DO UPDATE SET
                email = EXCLUDED.email,
                name = EXCLUDED.name,
                google_id = EXCLUDED.google_id,
                updated_at = NOW()
            """,
            user_id,
            email,
            name,
            f"mock-google-{user_id.hex[:12]}",
        )
    return ids


async def _upsert_groups_and_members(conn, user_ids: list[UUID]) -> dict[str, object]:
    groups = [
        {
            "id": _stable_uuid("group:campus-foodies"),
            "name": "Campus Foodies",
            "lead_id": user_ids[0],
            "member_ids": user_ids[:3],
            "locations": ["Boston, MA", "Cambridge, MA", "Somerville, MA"],
            "likes": [
                ["brunch", "cafe", "indoor"],
                ["restaurant", "food", "chill"],
                ["dessert", "cafe", "conversation"],
            ],
            "budget": ["budget", "student", "mid"],
        },
        {
            "id": _stable_uuid("group:weekend-explorers"),
            "name": "Weekend Explorers",
            "lead_id": user_ids[3],
            "member_ids": user_ids[3:],
            "locations": ["Boston, MA", "Brookline, MA", "Quincy, MA"],
            "likes": [
                ["hike", "outdoor", "activity"],
                ["park", "trail", "outdoor"],
                ["museum", "indoor", "chill"],
            ],
            "budget": ["mid", "premium", "budget"],
        },
    ]

    for group in groups:
        await conn.execute(
            """
            INSERT INTO groups (id, name, lead_id, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name,
                lead_id = EXCLUDED.lead_id,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            group["id"],
            group["name"],
            group["lead_id"],
        )

        for index, user_id in enumerate(group["member_ids"]):
            member_id = _stable_uuid(f"group-member:{group['id']}:{user_id}")
            role = "lead" if user_id == group["lead_id"] else "member"
            await conn.execute(
                """
                INSERT INTO group_members (id, group_id, user_id, status, role)
                VALUES ($1, $2, $3, 'active', $4)
                ON CONFLICT (group_id, user_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    role = EXCLUDED.role
                """,
                member_id,
                group["id"],
                user_id,
                role,
            )

            pref_id = _stable_uuid(f"group-pref:{group['id']}:{user_id}")
            await conn.execute(
                """
                INSERT INTO group_preferences (
                    id,
                    group_id,
                    user_id,
                    default_location,
                    activity_likes,
                    activity_dislikes,
                    meetup_frequency,
                    budget_preference,
                    notes
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)
                ON CONFLICT (group_id, user_id)
                DO UPDATE SET
                    default_location = EXCLUDED.default_location,
                    activity_likes = EXCLUDED.activity_likes,
                    activity_dislikes = EXCLUDED.activity_dislikes,
                    meetup_frequency = EXCLUDED.meetup_frequency,
                    budget_preference = EXCLUDED.budget_preference,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                pref_id,
                group["id"],
                user_id,
                group["locations"][index],
                json.dumps(group["likes"][index]),
                json.dumps(["crowded", "late-night"]),
                "weekly",
                group["budget"][index],
                "Auto-seeded for pipeline runs",
            )

    return {"groups": groups}


async def _upsert_availability(conn, user_ids: list[UUID]) -> None:
    for user_index, user_id in enumerate(user_ids):
        for day in (2, 4, 6):
            block_id = _stable_uuid(f"availability:{user_id}:{day}")
            start_hour = 9 + ((user_index + day) % 4)
            end_hour = start_hour + 2
            await conn.execute(
                """
                INSERT INTO availability_blocks (
                    id,
                    user_id,
                    day_of_week,
                    start_time,
                    end_time,
                    label,
                    location
                )
                VALUES ($1, $2, $3, $4::time, $5::time, $6, $7)
                ON CONFLICT (id)
                DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    label = EXCLUDED.label,
                    location = EXCLUDED.location
                """,
                block_id,
                user_id,
                day,
                f"{start_hour:02d}:00",
                f"{end_hour:02d}:00",
                "Busy",
                "Mock calendar block",
            )


async def _upsert_rounds_plans_votes_and_feedback(
    conn, groups_payload: dict[str, object]
) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    inserted_rounds = 0
    inserted_plans = 0
    inserted_votes = 0
    inserted_events = 0
    inserted_feedback = 0

    for group_index, group in enumerate(groups_payload["groups"]):
        group_id = group["id"]
        member_ids: list[UUID] = list(group["member_ids"])

        for iteration in (1, 2):
            round_id = _stable_uuid(f"plan-round:{group_id}:{iteration}")
            voting_deadline = now + timedelta(days=(group_index * 2 + iteration))

            await conn.execute(
                """
                INSERT INTO plan_rounds (id, group_id, iteration, status, voting_deadline, winning_plan_id)
                VALUES ($1, $2, $3, 'closed', $4, NULL)
                ON CONFLICT (id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    voting_deadline = EXCLUDED.voting_deadline
                """,
                round_id,
                group_id,
                iteration,
                voting_deadline,
            )
            inserted_rounds += 1

            plan_ids: list[UUID] = []
            vibe_options = (
                ["chill", "anchor", "reach"]
                if group_index == 0
                else ["hype", "wildcard", "pivot"]
            )
            venue_options = (
                ["Central Cafe", "River Park", "North Museum"]
                if group_index == 0
                else ["Skyline Trail", "City Arcade", "Harbor Grill"]
            )
            cost_options = ["budget", "mid", "premium"]

            for option in range(3):
                plan_id = _stable_uuid(f"plan:{round_id}:{option}")
                plan_ids.append(plan_id)
                await conn.execute(
                    """
                    INSERT INTO plans (
                        id,
                        plan_round_id,
                        title,
                        description,
                        vibe_type,
                        date_time,
                        location,
                        venue_name,
                        estimated_cost,
                        logistics,
                        raw_data
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb)
                    ON CONFLICT (id)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        vibe_type = EXCLUDED.vibe_type,
                        date_time = EXCLUDED.date_time,
                        location = EXCLUDED.location,
                        venue_name = EXCLUDED.venue_name,
                        estimated_cost = EXCLUDED.estimated_cost,
                        logistics = EXCLUDED.logistics,
                        raw_data = EXCLUDED.raw_data
                    """,
                    plan_id,
                    round_id,
                    f"Mock Plan {iteration}-{option + 1}",
                    "Synthetic plan generated for local DAG processing",
                    vibe_options[option],
                    now + timedelta(days=(iteration * 3 + option)),
                    group["locations"][option],
                    venue_options[option],
                    cost_options[option],
                    json.dumps(
                        {
                            "travel_minutes": 10 + option * 8,
                            "group_size": len(member_ids),
                        }
                    ),
                    json.dumps({"seeded": True, "source": "analytics.mock_seed"}),
                )
                inserted_plans += 1

            winning_plan_id = plan_ids[iteration % len(plan_ids)]
            await conn.execute(
                "UPDATE plan_rounds SET winning_plan_id = $2 WHERE id = $1",
                round_id,
                winning_plan_id,
            )

            for member_index, user_id in enumerate(member_ids):
                vote_id = _stable_uuid(f"vote:{round_id}:{user_id}")
                ranking = plan_ids[member_index % 3 :] + plan_ids[: member_index % 3]
                await conn.execute(
                    """
                    INSERT INTO votes (id, plan_round_id, user_id, rankings, notes)
                    VALUES ($1, $2, $3, $4::jsonb, $5)
                    ON CONFLICT (plan_round_id, user_id)
                    DO UPDATE SET
                        rankings = EXCLUDED.rankings,
                        notes = EXCLUDED.notes
                    """,
                    vote_id,
                    round_id,
                    user_id,
                    json.dumps([str(plan_id) for plan_id in ranking]),
                    "Auto-seeded vote for analytics pipeline",
                )
                inserted_votes += 1

            event_id = _stable_uuid(f"event:{round_id}")
            await conn.execute(
                """
                INSERT INTO events (id, group_id, plan_id, plan_round_id, google_calendar_event_id, event_date)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (plan_round_id)
                DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    plan_id = EXCLUDED.plan_id,
                    google_calendar_event_id = EXCLUDED.google_calendar_event_id,
                    event_date = EXCLUDED.event_date
                """,
                event_id,
                group_id,
                winning_plan_id,
                round_id,
                f"mock-google-event-{event_id.hex[:10]}",
                now + timedelta(days=(iteration * 4)),
            )
            inserted_events += 1

            ratings = ["loved", "liked", "neutral"]
            for member_index, user_id in enumerate(member_ids):
                feedback_id = _stable_uuid(f"feedback:{event_id}:{user_id}")
                await conn.execute(
                    """
                    INSERT INTO feedback (id, event_id, user_id, rating, notes, attended)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (event_id, user_id)
                    DO UPDATE SET
                        rating = EXCLUDED.rating,
                        notes = EXCLUDED.notes,
                        attended = EXCLUDED.attended
                    """,
                    feedback_id,
                    event_id,
                    user_id,
                    ratings[(iteration + member_index) % len(ratings)],
                    "Auto-seeded feedback for analytics materialization",
                    member_index != 2,
                )
                inserted_feedback += 1

    return {
        "rounds": inserted_rounds,
        "plans": inserted_plans,
        "votes": inserted_votes,
        "events": inserted_events,
        "feedback": inserted_feedback,
    }


async def ensure_mock_pipeline_source_data(min_plan_rows: int = 6) -> dict[str, object]:
    """Ensure operational source rows exist for analytics DAG materialization."""
    await ensure_analytics_schema()

    existing = {
        "plans": int(await db.fetchval("SELECT COUNT(*)::int FROM plans") or 0),
        "votes": int(await db.fetchval("SELECT COUNT(*)::int FROM votes") or 0),
        "feedback": int(await db.fetchval("SELECT COUNT(*)::int FROM feedback") or 0),
        "group_preferences": int(
            await db.fetchval("SELECT COUNT(*)::int FROM group_preferences") or 0
        ),
    }

    has_enough = (
        existing["plans"] >= min_plan_rows
        and existing["votes"] > 0
        and existing["feedback"] > 0
        and existing["group_preferences"] > 0
    )
    if has_enough:
        return {
            "seeded": False,
            "reason": "existing_source_data_available",
            "existing": existing,
        }

    async with db.acquire() as conn:
        async with conn.transaction():
            users = await _upsert_users(conn)
            groups_payload = await _upsert_groups_and_members(conn, users)
            await _upsert_availability(conn, users)
            upserted = await _upsert_rounds_plans_votes_and_feedback(
                conn, groups_payload
            )

    refreshed = {
        "plans": int(await db.fetchval("SELECT COUNT(*)::int FROM plans") or 0),
        "votes": int(await db.fetchval("SELECT COUNT(*)::int FROM votes") or 0),
        "feedback": int(await db.fetchval("SELECT COUNT(*)::int FROM feedback") or 0),
        "group_preferences": int(
            await db.fetchval("SELECT COUNT(*)::int FROM group_preferences") or 0
        ),
    }

    return {
        "seeded": True,
        "reason": "seeded_due_to_sparse_source_data",
        "existing": existing,
        "upserted": upserted,
        "current": refreshed,
    }

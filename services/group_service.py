"""Group domain service (business logic + persistence orchestration)."""

from __future__ import annotations

import json
from uuid import UUID

from database import db
from services.errors import BadRequestError, NotFoundError
from services.group_access import (
    get_user_email_or_404,
    require_active_group_member,
    require_group_lead,
)
from utils.email import send_invite_email

MAX_GROUP_MEMBERS = 4
MAX_INVITES_PER_REQUEST = 3


async def create_group(name: str, user_id: UUID) -> dict[str, object]:
    row = await db.fetchrow(
        """
        INSERT INTO groups (name, lead_id)
        VALUES ($1, $2)
        RETURNING id, name, lead_id, status
        """,
        name,
        user_id,
    )
    await db.execute(
        """
        INSERT INTO group_members (group_id, user_id, status, role)
        VALUES ($1, $2, 'active', 'lead')
        """,
        row["id"],
        user_id,
    )
    return {
        "group_id": str(row["id"]),
        "name": row["name"],
        "lead_id": str(row["lead_id"]),
        "members": [{"user_id": str(user_id), "role": "lead", "status": "active"}],
        "status": row["status"],
    }


async def list_groups(user_id: UUID) -> dict[str, list[dict[str, str]]]:
    groups = await db.fetch(
        """
        SELECT g.id, g.name, g.lead_id, g.status
        FROM groups g
        JOIN group_members gm ON g.id = gm.group_id
        WHERE gm.user_id = $1 AND gm.status = 'active'
        ORDER BY g.name
        """,
        user_id,
    )

    invites = await db.fetch(
        """
        SELECT gi.id, gi.group_id, g.name as group_name, u.name as inviter_name
        FROM group_invites gi
        JOIN groups g ON gi.group_id = g.id
        JOIN users u ON gi.invited_by = u.id
        WHERE gi.email = (SELECT email FROM users WHERE id = $1) AND gi.status = 'pending'
        """,
        user_id,
    )

    return {
        "groups": [
            {
                "id": str(g["id"]),
                "name": g["name"],
                "lead_id": str(g["lead_id"]),
                "status": g["status"],
            }
            for g in groups
        ],
        "pending_invites": [
            {
                "id": str(i["id"]),
                "group_id": str(i["group_id"]),
                "group_name": i["group_name"],
                "inviter_name": i["inviter_name"],
            }
            for i in invites
        ],
    }


async def get_group(group_id: UUID, user_id: UUID) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    group = await db.fetchrow(
        "SELECT id, name, lead_id, status FROM groups WHERE id = $1",
        group_id,
    )
    if not group:
        raise NotFoundError("Group not found")

    members = await db.fetch(
        """
        SELECT gm.id, gm.user_id, u.name, u.email, gm.status, gm.role
        FROM group_members gm
        JOIN users u ON gm.user_id = u.id
        WHERE gm.group_id = $1 AND gm.status = 'active'
        """,
        group_id,
    )

    rounds = await db.fetch(
        """
        SELECT id, iteration, status, voting_deadline, created_at
        FROM plan_rounds
        WHERE group_id = $1 AND status = 'voting_open'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        group_id,
    )

    events = await db.fetch(
        """
        SELECT e.id, e.event_date, p.title as plan_title
        FROM events e
        JOIN plans p ON e.plan_id = p.id
        WHERE e.group_id = $1
        ORDER BY e.event_date DESC
        LIMIT 10
        """,
        group_id,
    )

    prefs_row = await db.fetchrow(
        """
        SELECT default_location, activity_likes, activity_dislikes, meetup_frequency, budget_preference, notes
        FROM group_preferences
        WHERE group_id = $1 AND user_id = $2
        """,
        group_id,
        user_id,
    )
    preferences = {}
    if prefs_row:
        preferences = {
            "default_location": prefs_row["default_location"],
            "activity_likes": prefs_row["activity_likes"]
            if prefs_row["activity_likes"] is not None
            else [],
            "activity_dislikes": prefs_row["activity_dislikes"]
            if prefs_row["activity_dislikes"] is not None
            else [],
            "meetup_frequency": prefs_row["meetup_frequency"],
            "budget_preference": prefs_row["budget_preference"],
            "notes": prefs_row["notes"],
        }

    invites = await db.fetch(
        """
        SELECT id, email, status, created_at
        FROM group_invites
        WHERE group_id = $1
        ORDER BY created_at DESC
        """,
        group_id,
    )

    active_member_count = len(members)
    pending_invite_count = sum(1 for invite in invites if invite["status"] == "pending")
    slots_remaining = max(
        0, MAX_GROUP_MEMBERS - active_member_count - pending_invite_count
    )

    return {
        "group_id": str(group["id"]),
        "name": group["name"],
        "lead_id": str(group["lead_id"]),
        "is_lead": str(group["lead_id"]) == str(user_id),
        "members": [
            {
                "id": str(m["id"]),
                "user_id": str(m["user_id"]),
                "name": m["name"],
                "email": m["email"],
                "status": m["status"],
                "role": m["role"],
            }
            for m in members
        ],
        "invites": [
            {
                "id": str(i["id"]),
                "email": i["email"],
                "status": i["status"],
                "created_at": i["created_at"].isoformat() if i["created_at"] else None,
            }
            for i in invites
        ],
        "max_members": MAX_GROUP_MEMBERS,
        "slots_remaining": slots_remaining,
        "current_plans": [
            {
                "round_id": str(r["id"]),
                "iteration": r["iteration"],
                "status": r["status"],
                "voting_deadline": r["voting_deadline"].isoformat()
                if r["voting_deadline"]
                else None,
            }
            for r in rounds
        ],
        "events": [
            {
                "id": str(e["id"]),
                "event_date": e["event_date"].isoformat(),
                "plan_title": e["plan_title"],
            }
            for e in events
        ],
        "preferences": preferences,
        "history": [],
    }


async def update_group(group_id: UUID, user_id: UUID, name: str | None) -> dict[str, str]:
    await require_group_lead(group_id, user_id, detail="Only group lead can update")

    if name:
        await db.execute(
            "UPDATE groups SET name = $1, updated_at = NOW() WHERE id = $2",
            name,
            group_id,
        )

    group = await db.fetchrow(
        "SELECT id, name, status FROM groups WHERE id = $1",
        group_id,
    )
    if not group:
        raise NotFoundError("Group not found")

    return {
        "group_id": str(group["id"]),
        "name": group["name"],
        "status": group["status"],
    }


async def invite_members(
    group_id: UUID,
    user_id: UUID,
    emails: list[str],
) -> dict[str, list[dict[str, object]]]:
    if len(emails) > MAX_INVITES_PER_REQUEST:
        raise BadRequestError(
            f"Maximum {MAX_INVITES_PER_REQUEST} invites per request"
        )

    await require_group_lead(group_id, user_id, detail="Only group lead can invite")

    active_count = await db.fetchval(
        "SELECT COUNT(*) FROM group_members WHERE group_id = $1 AND status = 'active'",
        group_id,
    )
    pending_count = await db.fetchval(
        "SELECT COUNT(*) FROM group_invites WHERE group_id = $1 AND status = 'pending'",
        group_id,
    )

    slots_remaining = MAX_GROUP_MEMBERS - (active_count or 0) - (pending_count or 0)
    if slots_remaining <= 0:
        raise BadRequestError(
            "Group is full. Maximum 4 members (including pending invites)."
        )

    normalized_emails = [email.strip().lower() for email in emails if email.strip()]
    emails_to_invite = normalized_emails[:slots_remaining]

    existing_member_emails = await db.fetch(
        """
        SELECT u.email FROM group_members gm
        JOIN users u ON gm.user_id = u.id
        WHERE gm.group_id = $1 AND gm.status = 'active'
        """,
        group_id,
    )
    member_email_set = {r["email"].lower() for r in existing_member_emails}

    pending_invite_emails = await db.fetch(
        "SELECT email FROM group_invites WHERE group_id = $1 AND status = 'pending'",
        group_id,
    )
    pending_email_set = {r["email"].lower() for r in pending_invite_emails}

    inviter = await db.fetchrow("SELECT name, email FROM users WHERE id = $1", user_id)
    if not inviter:
        raise NotFoundError("User not found")
    inviter_name = inviter["name"] or inviter["email"].split("@")[0]

    group = await db.fetchrow("SELECT name FROM groups WHERE id = $1", group_id)
    group_name = group["name"] if group else "a group"

    invites_sent: list[dict[str, object]] = []
    for email in emails_to_invite:
        if email in member_email_set:
            invites_sent.append({"email": email, "status": "already_member"})
            continue

        if email in pending_email_set:
            invites_sent.append({"email": email, "status": "already_invited"})
            continue

        try:
            await db.execute(
                """
                INSERT INTO group_invites (group_id, email, invited_by)
                VALUES ($1, $2, $3)
                ON CONFLICT (group_id, email) DO UPDATE SET
                    status = 'pending',
                    invited_by = EXCLUDED.invited_by,
                    created_at = NOW()
                """,
                group_id,
                email,
                user_id,
            )

            email_sent = send_invite_email(
                to_email=email,
                group_name=group_name,
                inviter_name=inviter_name,
                group_id=str(group_id),
            )
            invites_sent.append(
                {
                    "email": email,
                    "status": "pending",
                    "email_sent": email_sent,
                }
            )
        except Exception:
            invites_sent.append({"email": email, "status": "error"})

    return {"invites_sent": invites_sent}


async def accept_invite(group_id: UUID, user_id: UUID) -> dict[str, str]:
    user_email = await get_user_email_or_404(user_id)

    invite = await db.fetchrow(
        "SELECT id FROM group_invites WHERE group_id = $1 AND email = $2 AND status = 'pending'",
        group_id,
        user_email,
    )
    if not invite:
        raise NotFoundError("No pending invite found")

    await db.execute(
        "UPDATE group_invites SET status = 'accepted' WHERE id = $1",
        invite["id"],
    )
    await db.execute(
        """
        INSERT INTO group_members (group_id, user_id, status, role)
        VALUES ($1, $2, 'active', 'member')
        ON CONFLICT (group_id, user_id) DO UPDATE SET status = 'active'
        """,
        group_id,
        user_id,
    )
    return {"group_id": str(group_id), "member_status": "active"}


async def reject_invite(group_id: UUID, user_id: UUID) -> dict[str, str]:
    user_email = await get_user_email_or_404(user_id)

    await db.execute(
        """
        UPDATE group_invites SET status = 'rejected'
        WHERE group_id = $1 AND email = $2 AND status = 'pending'
        """,
        group_id,
        user_email,
    )
    return {"group_id": str(group_id), "member_status": "rejected"}


async def update_group_preferences(
    group_id: UUID,
    user_id: UUID,
    updates: dict[str, object],
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    if not updates:
        return {"group_id": str(group_id), "user_id": str(user_id), "preferences": {}}

    activity_likes = (
        json.dumps(updates["activity_likes"])
        if "activity_likes" in updates
        else None
    )
    activity_dislikes = (
        json.dumps(updates["activity_dislikes"])
        if "activity_dislikes" in updates
        else None
    )

    await db.execute(
        """
        INSERT INTO group_preferences (group_id, user_id, default_location, activity_likes, activity_dislikes, meetup_frequency, budget_preference, notes)
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8)
        ON CONFLICT (group_id, user_id) DO UPDATE SET
            default_location = COALESCE(EXCLUDED.default_location, group_preferences.default_location),
            activity_likes = COALESCE(EXCLUDED.activity_likes, group_preferences.activity_likes),
            activity_dislikes = COALESCE(EXCLUDED.activity_dislikes, group_preferences.activity_dislikes),
            meetup_frequency = COALESCE(EXCLUDED.meetup_frequency, group_preferences.meetup_frequency),
            budget_preference = COALESCE(EXCLUDED.budget_preference, group_preferences.budget_preference),
            notes = COALESCE(EXCLUDED.notes, group_preferences.notes),
            updated_at = NOW()
        """,
        group_id,
        user_id,
        updates.get("default_location"),
        activity_likes,
        activity_dislikes,
        updates.get("meetup_frequency"),
        updates.get("budget_preference"),
        updates.get("notes"),
    )

    return {
        "group_id": str(group_id),
        "user_id": str(user_id),
        "preferences": updates,
    }


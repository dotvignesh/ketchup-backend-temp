"""Group availability domain service."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from uuid import UUID

from database import db
from services.group_access import require_active_group_member


def _expand_blocks_to_intervals(
    blocks: list, time_min: datetime, time_max: datetime
) -> list[tuple[datetime, datetime]]:
    intervals = []
    for block in blocks:
        day_of_week = block["day_of_week"]
        start_t = block["start_time"]
        end_t = block["end_time"]

        if isinstance(start_t, str):
            hour, minute = map(int, start_t.split(":")[:2])
            start_t = time(hour, minute)
        if isinstance(end_t, str):
            hour, minute = map(int, end_t.split(":")[:2])
            end_t = time(hour, minute)

        current = time_min.replace(hour=0, minute=0, second=0, microsecond=0)
        target_weekday = (day_of_week + 6) % 7
        while current <= time_max:
            if current.weekday() == target_weekday:
                start_dt = datetime.combine(current.date(), start_t)
                end_dt = datetime.combine(current.date(), end_t)
                if start_dt < time_max and end_dt > time_min:
                    intervals.append((max(start_dt, time_min), min(end_dt, time_max)))
            current += timedelta(days=1)
    return intervals


def _merge_overlapping(
    intervals: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    if not intervals:
        return []
    sorted_intervals = sorted(intervals)
    merged = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _split_slot_by_day(
    slot_start: datetime, slot_end: datetime, slot_hours: float
) -> list[dict]:
    split_slots = []
    current = slot_start
    while current < slot_end:
        day_end = current.replace(hour=23, minute=59, second=59, microsecond=999)
        chunk_end = min(day_end, slot_end)
        if (chunk_end - current).total_seconds() >= slot_hours * 3600:
            split_slots.append(
                {
                    "start": current.isoformat(),
                    "end": chunk_end.isoformat(),
                }
            )
        current = datetime.combine(chunk_end.date() + timedelta(days=1), time(0, 0, 0))
    return split_slots


def _find_common_free(
    all_busy: dict, time_min: datetime, time_max: datetime, slot_hours: float = 2
) -> list[dict]:
    if not all_busy:
        return [{"start": time_min.isoformat(), "end": time_max.isoformat()}]

    merged = {uid: _merge_overlapping(busy) for uid, busy in all_busy.items()}

    all_starts = []
    all_ends = []
    for busy_slots in merged.values():
        for start, end in busy_slots:
            all_starts.append(start)
            all_ends.append(end)

    if not all_starts:
        return [{"start": time_min.isoformat(), "end": time_max.isoformat()}]

    events = sorted([(start, 1) for start in all_starts] + [(end, -1) for end in all_ends])
    count = 0
    gap_start = None
    free_slots = []
    for moment, delta in events:
        count += delta
        if count == 0:
            gap_start = moment
        elif gap_start and count == 1:
            if (moment - gap_start).total_seconds() >= slot_hours * 3600:
                if (moment - gap_start).total_seconds() > 24 * 3600:
                    free_slots.extend(_split_slot_by_day(gap_start, moment, slot_hours))
                else:
                    free_slots.append(
                        {
                            "start": gap_start.isoformat(),
                            "end": moment.isoformat(),
                        }
                    )
            gap_start = None
    return free_slots[:15]


def _parse_window(
    time_min: str | None,
    time_max: str | None,
) -> tuple[datetime, datetime]:
    now = datetime.utcnow()
    start = datetime.fromisoformat(time_min.replace("Z", "+00:00")) if time_min else now
    end = (
        datetime.fromisoformat(time_max.replace("Z", "+00:00"))
        if time_max
        else now + timedelta(days=7)
    )
    return start, end


async def compute_group_availability(
    group_id: UUID,
    user_id: UUID,
    time_min: str | None,
    time_max: str | None,
) -> dict[str, object]:
    await require_active_group_member(group_id, user_id)

    start, end = _parse_window(time_min, time_max)

    members = await db.fetch(
        "SELECT user_id FROM group_members WHERE group_id = $1 AND status = 'active'",
        group_id,
    )

    all_busy = {}
    for member in members:
        blocks = await db.fetch(
            """
            SELECT day_of_week, start_time, end_time
            FROM availability_blocks
            WHERE user_id = $1
            """,
            member["user_id"],
        )
        intervals = _expand_blocks_to_intervals([dict(block) for block in blocks], start, end)
        all_busy[str(member["user_id"])] = intervals

    common_free = _find_common_free(all_busy, start, end)
    return {
        "common_slots": common_free,
        "per_user_busy": {uid: len(busy) for uid, busy in all_busy.items()},
    }


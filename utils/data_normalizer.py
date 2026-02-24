"""Normalization and validation helpers for calendar, venue, and route data."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from models.schemas import (
    CalendarData,
    FreeBusyInterval,
    TravelRoute,
    VenueLocation,
    VenueMetadata,
)

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Transform external API payloads into internal schema objects."""

    @staticmethod
    def normalize_calendar_data(
        user_id: str,
        raw_calendar_response: dict[str, Any],
        calendar_id: str | None = None,
    ) -> CalendarData:
        try:
            intervals: list[FreeBusyInterval] = []
            for busy_period in raw_calendar_response.get("busy", []):
                start = datetime.fromisoformat(busy_period["start"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(busy_period["end"].replace("Z", "+00:00"))
                intervals.append(FreeBusyInterval(start=start, end=end, busy=True))

            calendar_data = CalendarData(
                user_id=user_id,
                intervals=intervals,
                retrieved_at=datetime.now(timezone.utc),
                calendar_id=calendar_id,
            )
            logger.info(
                "Normalized calendar data user_id=%s intervals=%s",
                user_id,
                len(intervals),
            )
            return calendar_data
        except Exception:
            logger.exception("Failed to normalize calendar data user_id=%s", user_id)
            raise

    @staticmethod
    def normalize_google_place(place_data: dict[str, Any]) -> VenueMetadata:
        try:
            location_data = place_data.get("location", {})
            location = VenueLocation(
                latitude=location_data.get("latitude", 0),
                longitude=location_data.get("longitude", 0),
                address=place_data.get("formatted_address", ""),
                city="",
                state="",
                zip_code="",
            )

            photos = [
                "https://maps.googleapis.com/maps/api/place/photo"
                f"?maxwidth=400&photo_reference={photo['photo_reference']}"
                for photo in place_data.get("photos", [])
            ]
            place_types = place_data.get("types", [])
            category = place_types[0].replace("_", " ").title() if place_types else "Other"

            venue = VenueMetadata(
                venue_id=place_data.get("place_id") or place_data.get("id"),
                name=place_data.get("name") or "Unknown venue",
                category=category,
                rating=place_data.get("rating", 0),
                review_count=place_data.get("user_ratings_total", 0),
                price_level=place_data.get("price_level"),
                location=location,
                photos=photos[:3],
                source="google_places",
                source_url=place_data.get("url", ""),
                retrieved_at=datetime.now(timezone.utc),
            )
            logger.info("Normalized Google Place venue_id=%s", venue.venue_id)
            return venue
        except Exception:
            logger.exception("Failed to normalize Google Place payload")
            raise

    @staticmethod
    def normalize_route(
        origin_user_id: str,
        destination_venue_id: str,
        route_data: dict[str, Any],
    ) -> TravelRoute:
        try:
            legs = route_data.get("legs", [])
            leg = legs[0] if legs else route_data

            distance_meters = leg.get("distanceMeters")
            if distance_meters is None:
                distance_text = leg.get("distance", {}).get("text", "0 mi")
                distance_miles = float(distance_text.split()[0])
            else:
                distance_miles = float(distance_meters) / 1609.34

            duration_raw = leg.get("duration")
            if isinstance(duration_raw, str) and duration_raw.endswith("s"):
                duration_minutes = int(round(float(duration_raw[:-1]) / 60.0))
            else:
                duration_text = (
                    duration_raw.get("text", "0 mins")
                    if isinstance(duration_raw, dict)
                    else "0 mins"
                )
                duration_minutes = int(duration_text.split()[0])

            route = TravelRoute(
                origin_user_id=origin_user_id,
                destination_venue_id=destination_venue_id,
                distance_miles=distance_miles,
                duration_minutes=duration_minutes,
                retrieved_at=datetime.now(timezone.utc),
            )
            logger.info(
                "Normalized route origin=%s destination=%s distance_miles=%.2f duration_minutes=%s",
                origin_user_id,
                destination_venue_id,
                distance_miles,
                duration_minutes,
            )
            return route
        except Exception:
            logger.exception(
                "Failed to normalize route origin=%s destination=%s",
                origin_user_id,
                destination_venue_id,
            )
            raise

    @staticmethod
    def validate_schema(data: dict[str, Any], schema_class: type[Any]) -> bool:
        try:
            schema_class(**data)
            return True
        except Exception as exc:
            logger.warning("Schema validation failed: %s", exc)
            return False

    @staticmethod
    def deduplicate_venues(venues: list[VenueMetadata]) -> list[VenueMetadata]:
        seen: dict[tuple[str, float, float], bool] = {}
        unique: list[VenueMetadata] = []

        for venue in venues:
            key = (
                venue.name,
                round(venue.location.latitude, 4),
                round(venue.location.longitude, 4),
            )
            if key in seen:
                continue
            seen[key] = True
            unique.append(venue)

        logger.info("Deduplicated venues input=%s output=%s", len(venues), len(unique))
        return unique

    @staticmethod
    def compress_event_options(
        options: list[dict[str, Any]],
        max_tokens: int = 2000,
    ) -> str:
        lines = ["Event Options Summary:"]
        for idx, option in enumerate(options, 1):
            lines.append(f"\n{idx}. {option.get('title', 'Unknown')}")
            lines.append(f"   Vibe: {option.get('vibe_category', 'N/A')}")
            lines.append(f"   Location: {option.get('venue', {}).get('name', 'N/A')}")
            lines.append(f"   Cost: ${option.get('estimated_cost_per_person', 0)}")
            lines.append(
                f"   Duration: {option.get('estimated_duration_minutes', 0)} min",
            )

        summary = "\n".join(lines)
        max_chars = max_tokens * 4
        truncated = summary[:max_chars]
        logger.info("Compressed options count=%s chars=%s", len(options), len(truncated))
        return truncated


class DataValidator:
    """Validation helpers for normalized entities."""

    @staticmethod
    def validate_calendar_intervals(intervals: list[FreeBusyInterval]) -> bool:
        try:
            for index, interval in enumerate(intervals):
                if interval.end <= interval.start:
                    logger.warning("Invalid interval index=%s end<=start", index)
                    return False

                for next_index in range(index + 1, len(intervals)):
                    other = intervals[next_index]
                    if interval.start < other.end and interval.end > other.start:
                        logger.warning(
                            "Overlapping intervals index_a=%s index_b=%s",
                            index,
                            next_index,
                        )
                        return False

            return True
        except Exception:
            logger.exception("Calendar interval validation failed")
            return False

    @staticmethod
    def validate_venue_metadata(venue: VenueMetadata) -> bool:
        try:
            if not venue.name or not venue.name.strip():
                logger.warning("Venue validation failed: missing name")
                return False

            if venue.rating < 0 or venue.rating > 5:
                logger.warning("Venue validation failed: rating=%s", venue.rating)
                return False

            if venue.location.latitude < -90 or venue.location.latitude > 90:
                logger.warning(
                    "Venue validation failed: latitude=%s",
                    venue.location.latitude,
                )
                return False

            if venue.location.longitude < -180 or venue.location.longitude > 180:
                logger.warning(
                    "Venue validation failed: longitude=%s",
                    venue.location.longitude,
                )
                return False

            return True
        except Exception:
            logger.exception("Venue metadata validation failed")
            return False

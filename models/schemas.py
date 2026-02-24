"""Pydantic schemas for API request/response."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# Auth
class GoogleSigninRequest(BaseModel):
    email: str
    name: Optional[str] = None
    google_id: Optional[str] = None


class GoogleSigninResponse(BaseModel):
    user_id: UUID
    email: str
    name: Optional[str] = None


# Users
class UserResponse(BaseModel):
    id: UUID
    email: str
    name: Optional[str] = None
    google_calendar_connected: bool = False

    class Config:
        from_attributes = True


class UserPreferencesUpdate(BaseModel):
    default_location: Optional[str] = None
    activity_likes: Optional[list[str]] = None
    activity_dislikes: Optional[list[str]] = None


# Groups
class GroupCreate(BaseModel):
    name: str


class GroupUpdate(BaseModel):
    name: Optional[str] = None


class GroupMemberResponse(BaseModel):
    id: UUID
    user_id: UUID
    name: Optional[str] = None
    email: str
    status: str
    role: str


class GroupResponse(BaseModel):
    id: UUID
    name: str
    lead_id: UUID
    status: str
    members: list[GroupMemberResponse] = Field(default_factory=list)
    created_at: Optional[datetime] = None


class GroupInviteRequest(BaseModel):
    emails: list[str]


class GroupPreferencesUpdate(BaseModel):
    default_location: Optional[str] = None
    activity_likes: Optional[list[str]] = None
    activity_dislikes: Optional[list[str]] = None
    meetup_frequency: Optional[str] = None
    budget_preference: Optional[str] = None
    notes: Optional[str] = None


# Plans
class PlanResponse(BaseModel):
    id: UUID
    title: str
    description: Optional[str] = None
    vibe_type: Optional[str] = None
    date_time: Optional[datetime] = None
    location: Optional[str] = None
    venue_name: Optional[str] = None
    estimated_cost: Optional[str] = None
    logistics: Optional[dict] = None


class PlanRoundResponse(BaseModel):
    id: UUID
    group_id: UUID
    iteration: int
    status: str
    voting_deadline: Optional[datetime] = None
    plans: list[PlanResponse] = Field(default_factory=list)


class VoteRequest(BaseModel):
    rankings: list[UUID]
    notes: Optional[str] = None


class RefinePlansRequest(BaseModel):
    descriptors: list[str] = Field(default_factory=list)
    lead_note: Optional[str] = None


# Availability
class AvailabilityBlockCreate(BaseModel):
    day_of_week: int
    start_time: str
    end_time: str
    label: Optional[str] = None  # e.g. "Work", "Class"
    location: Optional[str] = None  # e.g. "Snell Library", "Home"


class AvailabilityBlockResponse(BaseModel):
    id: UUID
    day_of_week: int
    start_time: str
    end_time: str
    label: Optional[str] = None
    location: Optional[str] = None


class AvailabilityBlocksUpdate(BaseModel):
    blocks: list[AvailabilityBlockCreate] = Field(default_factory=list)


# Feedback
class FeedbackCreate(BaseModel):
    rating: str
    notes: Optional[str] = None
    attended: bool = True


# Pipeline schemas
class FreeBusyInterval(BaseModel):
    start: datetime
    end: datetime
    busy: bool = True


class CalendarData(BaseModel):
    user_id: str
    intervals: list[FreeBusyInterval] = Field(default_factory=list)
    retrieved_at: datetime
    calendar_id: Optional[str] = None


class VenueLocation(BaseModel):
    latitude: float
    longitude: float
    address: str
    city: str
    state: str
    zip_code: str


class VenueMetadata(BaseModel):
    venue_id: str
    name: str
    category: str
    rating: float
    review_count: int
    price_level: Optional[int] = None
    location: VenueLocation
    photos: list[str] = Field(default_factory=list)
    source: str
    source_url: str
    retrieved_at: datetime


class TravelRoute(BaseModel):
    origin_user_id: str
    destination_venue_id: str
    distance_miles: float
    duration_minutes: int
    retrieved_at: datetime


class EventOption(BaseModel):
    title: str
    vibe_category: str
    venue: VenueMetadata
    estimated_cost_per_person: Optional[float] = None
    estimated_duration_minutes: Optional[int] = None

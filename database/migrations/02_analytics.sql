-- Analytics schema for pipeline-to-runtime integration.
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    row_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary TEXT,
    version_sha TEXT
);

CREATE INDEX IF NOT EXISTS idx_analytics_pipeline_runs_job_started
    ON analytics.pipeline_runs (job_name, started_at DESC);

CREATE TABLE IF NOT EXISTS analytics.group_feature_snapshot (
    group_id UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feature_version TEXT NOT NULL,
    top_activity_tags TEXT[] NOT NULL DEFAULT '{}'::text[],
    budget_mode TEXT,
    mobility_mode TEXT,
    historical_novelty_score NUMERIC(5,4),
    refine_descriptor_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_latest BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (group_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_analytics_group_snapshot_latest
    ON analytics.group_feature_snapshot (group_id, is_latest, snapshot_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_analytics_group_snapshot_one_latest
    ON analytics.group_feature_snapshot (group_id)
    WHERE is_latest = TRUE;

CREATE TABLE IF NOT EXISTS analytics.venue_performance_prior (
    group_id UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    venue_key TEXT NOT NULL,
    win_rate NUMERIC(5,4) NOT NULL DEFAULT 0,
    avg_rank NUMERIC(6,3),
    attendance_rate NUMERIC(5,4),
    feedback_score NUMERIC(5,4),
    sample_size INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, venue_key)
);

CREATE INDEX IF NOT EXISTS idx_analytics_venue_priors_group
    ON analytics.venue_performance_prior (group_id, sample_size DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS analytics.plan_outcome_fact (
    plan_id UUID PRIMARY KEY REFERENCES plans(id) ON DELETE CASCADE,
    group_id UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    plan_round_id UUID NOT NULL REFERENCES plan_rounds(id) ON DELETE CASCADE,
    iteration INTEGER,
    won BOOLEAN NOT NULL DEFAULT FALSE,
    avg_vote_rank NUMERIC(6,3),
    attended_rate NUMERIC(5,4),
    feedback_loved_rate NUMERIC(5,4),
    cost_bucket TEXT,
    vibe_type TEXT,
    venue_key TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_analytics_plan_outcome_group_created
    ON analytics.plan_outcome_fact (group_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_analytics_plan_outcome_group_round
    ON analytics.plan_outcome_fact (group_id, plan_round_id);

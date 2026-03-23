from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_text TEXT NOT NULL,
    platform TEXT NOT NULL,
    persona TEXT NOT NULL,
    selected_objective TEXT,
    selected_tactic TEXT,
    exit_state TEXT,
    analysis_json TEXT NOT NULL,
    plan_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    rank_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    tactic TEXT,
    objective TEXT NOT NULL,
    estimated_bite_score REAL NOT NULL,
    estimated_audience_score REAL NOT NULL,
    critic_penalty REAL NOT NULL,
    rank_score REAL NOT NULL,
    critic_notes_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL UNIQUE,
    got_reply INTEGER NOT NULL,
    reply_delay_seconds INTEGER,
    reply_length INTEGER,
    tone_shift TEXT,
    spectator_engagement INTEGER,
    result_label TEXT,
    notes TEXT,
    emit_outbox_id INTEGER,
    emit_dispatch_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (emit_outbox_id) REFERENCES emit_outbox(id) ON DELETE SET NULL,
    FOREIGN KEY (emit_dispatch_id) REFERENCES emit_dispatches(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS panel_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id INTEGER NOT NULL,
    platform TEXT NOT NULL,
    persona TEXT NOT NULL,
    candidate_tactic TEXT,
    candidate_objective TEXT,
    selection_preset TEXT,
    selection_strategy TEXT,
    disposition TEXT NOT NULL,
    notes TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS emit_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id INTEGER NOT NULL,
    platform TEXT NOT NULL,
    transport TEXT NOT NULL,
    selection_preset TEXT,
    selection_strategy TEXT,
    tactic TEXT,
    objective TEXT,
    status TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    emit_request_json TEXT NOT NULL,
    notes TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    retry_policy_json TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS emit_dispatches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    emit_outbox_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    driver TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    notes TEXT,
    FOREIGN KEY (emit_outbox_id) REFERENCES emit_outbox(id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dispatch_circuit_breakers (
    scope_key TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    failure_timestamps_json TEXT NOT NULL,
    opened_at TEXT,
    open_until TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dispatch_control_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    governor_json TEXT NOT NULL,
    containment_json TEXT NOT NULL,
    escalation_json TEXT NOT NULL,
    safety_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_control_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    governor_json TEXT NOT NULL,
    containment_json TEXT NOT NULL,
    escalation_json TEXT NOT NULL,
    safety_json TEXT NOT NULL,
    telemetry_json TEXT NOT NULL,
    is_last_good INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS intake_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_driver TEXT NOT NULL,
    source_item_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    reply_to_id TEXT,
    author_handle TEXT,
    subject TEXT,
    body TEXT NOT NULL,
    permalink TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    score_json TEXT NOT NULL,
    analysis_json TEXT NOT NULL,
    context_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    promoted_run_id INTEGER,
    emit_outbox_id INTEGER,
    FOREIGN KEY (promoted_run_id) REFERENCES runs(id) ON DELETE SET NULL,
    FOREIGN KEY (emit_outbox_id) REFERENCES emit_outbox(id) ON DELETE SET NULL,
    UNIQUE(source_driver, source_item_id)
);

CREATE TABLE IF NOT EXISTS mutation_families (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id INTEGER NOT NULL,
    winner_candidate_id INTEGER,
    winner_rank_index INTEGER,
    persona TEXT NOT NULL,
    platform TEXT NOT NULL,
    tactic TEXT,
    objective TEXT,
    winner_score REAL NOT NULL DEFAULT 0,
    source TEXT,
    strategy TEXT NOT NULL,
    notes TEXT,
    lineage_json TEXT NOT NULL,
    UNIQUE(run_id, winner_candidate_id, strategy),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (winner_candidate_id) REFERENCES candidates(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS mutation_variants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    family_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    parent_candidate_id INTEGER,
    transform TEXT NOT NULL,
    variant_text TEXT NOT NULL,
    variant_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'drafted',
    score_json TEXT NOT NULL,
    lineage_json TEXT NOT NULL,
    UNIQUE(family_id, variant_hash),
    FOREIGN KEY (family_id) REFERENCES mutation_families(id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_candidate_id) REFERENCES candidates(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_candidates_run_rank ON candidates(run_id, rank_index);
CREATE INDEX IF NOT EXISTS idx_panel_reviews_run_id ON panel_reviews(run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_panel_reviews_basis ON panel_reviews(platform, persona, candidate_objective, candidate_tactic, disposition);
CREATE INDEX IF NOT EXISTS idx_emit_outbox_run_id ON emit_outbox(run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_emit_outbox_status ON emit_outbox(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_emit_outbox_retry_queue ON emit_outbox(status, next_retry_at, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_emit_dispatches_emit_id ON emit_dispatches(emit_outbox_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_emit_dispatches_status ON emit_dispatches(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dispatch_control_checkpoints_created_at ON dispatch_control_checkpoints(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_dispatch_control_checkpoints_last_good ON dispatch_control_checkpoints(is_last_good, id DESC);
CREATE INDEX IF NOT EXISTS idx_intake_targets_status ON intake_targets(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intake_targets_platform ON intake_targets(platform, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_intake_targets_run_id ON intake_targets(promoted_run_id);
CREATE INDEX IF NOT EXISTS idx_mutation_families_run_id ON mutation_families(run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mutation_families_basis ON mutation_families(persona, platform, tactic, objective, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mutation_variants_family_id ON mutation_variants(family_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mutation_variants_run_id ON mutation_variants(run_id, created_at DESC);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)

    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(emit_outbox)").fetchall()
    }
    if "attempt_count" not in existing_columns:
        conn.execute("ALTER TABLE emit_outbox ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0")
    if "next_retry_at" not in existing_columns:
        conn.execute("ALTER TABLE emit_outbox ADD COLUMN next_retry_at TEXT")
    if "retry_policy_json" not in existing_columns:
        conn.execute("ALTER TABLE emit_outbox ADD COLUMN retry_policy_json TEXT")

    outcome_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(outcomes)").fetchall()
    }
    if "emit_outbox_id" not in outcome_columns:
        conn.execute("ALTER TABLE outcomes ADD COLUMN emit_outbox_id INTEGER")
    if "emit_dispatch_id" not in outcome_columns:
        conn.execute("ALTER TABLE outcomes ADD COLUMN emit_dispatch_id INTEGER")

    intake_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(intake_targets)").fetchall()
    }
    if intake_columns:
        if "updated_at" not in intake_columns:
            conn.execute("ALTER TABLE intake_targets ADD COLUMN updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
        if "promoted_run_id" not in intake_columns:
            conn.execute("ALTER TABLE intake_targets ADD COLUMN promoted_run_id INTEGER")
        if "emit_outbox_id" not in intake_columns:
            conn.execute("ALTER TABLE intake_targets ADD COLUMN emit_outbox_id INTEGER")

    conn.commit()

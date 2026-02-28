"""SQLite schema creation and migration for the Asana archive."""

import sqlite3

SCHEMA_VERSION = 1

_SCHEMA_SQL = """\

-- Schema versioning
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Tracks which JSON files have been imported and their content hashes,
-- so re-import can skip unchanged files.
CREATE TABLE IF NOT EXISTS sync_state (
    source_path TEXT PRIMARY KEY,
    source_hash TEXT NOT NULL,
    last_import_at INTEGER NOT NULL
);

-- Asana users (assignees, creators, followers). Deduplicates user info
-- that appears inline on many tasks/stories.
CREATE TABLE IF NOT EXISTS users (
    gid TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    email TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS teams (
    gid TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    gid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    team_gid TEXT,
    archived INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    owner_gid TEXT,
    created_at TEXT,
    modified_at TEXT,
    due_date TEXT,
    permalink_url TEXT,
    FOREIGN KEY (team_gid) REFERENCES teams(gid),
    FOREIGN KEY (owner_gid) REFERENCES users(gid)
);

CREATE TABLE IF NOT EXISTS sections (
    gid TEXT PRIMARY KEY,
    project_gid TEXT NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (project_gid) REFERENCES projects(gid)
);

-- Unified table for tasks AND subtasks.
-- parent_gid IS NULL for top-level tasks; set for subtasks.
-- Denormalized fields (assignee_name, project_name, etc.) stored for
-- display convenience and FTS indexing.
CREATE TABLE IF NOT EXISTS tasks (
    gid TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    html_notes TEXT NOT NULL DEFAULT '',
    resource_subtype TEXT NOT NULL DEFAULT 'default_task',

    -- Hierarchy
    parent_gid TEXT,
    depth INTEGER NOT NULL DEFAULT 0,

    -- Denormalized context (avoids joins for display + FTS)
    project_gid TEXT,
    project_name TEXT NOT NULL DEFAULT '',
    team_gid TEXT,
    team_name TEXT NOT NULL DEFAULT '',
    section_gid TEXT,
    section_name TEXT NOT NULL DEFAULT '',

    -- Assignment
    assignee_gid TEXT,
    assignee_name TEXT NOT NULL DEFAULT '',

    -- Completion
    completed INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    completed_by_gid TEXT,

    -- Timestamps (ISO-8601 text, matching Asana's format)
    created_at TEXT,
    created_by_gid TEXT,
    modified_at TEXT,

    -- Dates
    due_on TEXT,
    due_at TEXT,
    start_on TEXT,
    start_at TEXT,

    -- Misc
    num_likes INTEGER NOT NULL DEFAULT 0,
    permalink_url TEXT,

    -- JSON array of tag name strings: ["urgent","bug"]
    tags TEXT NOT NULL DEFAULT '[]',

    -- JSON array of custom field objects
    custom_fields TEXT NOT NULL DEFAULT '[]',

    -- JSON arrays of {gid, name} objects
    dependencies TEXT NOT NULL DEFAULT '[]',
    dependents TEXT NOT NULL DEFAULT '[]',

    -- JSON array of user gid strings
    followers TEXT NOT NULL DEFAULT '[]',

    -- Full raw JSON for anything not explicitly modeled
    raw_json TEXT,

    FOREIGN KEY (parent_gid) REFERENCES tasks(gid),
    FOREIGN KEY (project_gid) REFERENCES projects(gid),
    FOREIGN KEY (team_gid) REFERENCES teams(gid),
    FOREIGN KEY (section_gid) REFERENCES sections(gid),
    FOREIGN KEY (assignee_gid) REFERENCES users(gid),
    FOREIGN KEY (completed_by_gid) REFERENCES users(gid),
    FOREIGN KEY (created_by_gid) REFERENCES users(gid)
);

-- A task can belong to multiple projects, each with its own section.
CREATE TABLE IF NOT EXISTS task_memberships (
    task_gid TEXT NOT NULL,
    project_gid TEXT NOT NULL,
    project_name TEXT NOT NULL DEFAULT '',
    section_gid TEXT,
    section_name TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (task_gid, project_gid),
    FOREIGN KEY (task_gid) REFERENCES tasks(gid),
    FOREIGN KEY (project_gid) REFERENCES projects(gid)
);

-- Comments and system events on a task.
CREATE TABLE IF NOT EXISTS stories (
    gid TEXT PRIMARY KEY,
    task_gid TEXT NOT NULL,
    created_at TEXT,
    created_by_gid TEXT,
    created_by_name TEXT NOT NULL DEFAULT '',
    resource_subtype TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL DEFAULT '',
    html_text TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (task_gid) REFERENCES tasks(gid),
    FOREIGN KEY (created_by_gid) REFERENCES users(gid)
);

CREATE TABLE IF NOT EXISTS attachments (
    gid TEXT PRIMARY KEY,
    task_gid TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    file_name TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    size INTEGER,
    created_at TEXT,
    download_url TEXT,
    permanent_url TEXT,
    uploaded_by_gid TEXT,
    uploaded_by_name TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (task_gid) REFERENCES tasks(gid),
    FOREIGN KEY (uploaded_by_gid) REFERENCES users(gid)
);

-- Indexes

CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_gid);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_gid);
CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee_gid);
CREATE INDEX IF NOT EXISTS idx_tasks_modified ON tasks(modified_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tasks(completed);
CREATE INDEX IF NOT EXISTS idx_stories_task ON stories(task_gid);
CREATE INDEX IF NOT EXISTS idx_stories_comments ON stories(task_gid)
    WHERE resource_subtype = 'comment';
CREATE INDEX IF NOT EXISTS idx_attachments_task ON attachments(task_gid);
CREATE INDEX IF NOT EXISTS idx_projects_team ON projects(team_gid);
CREATE INDEX IF NOT EXISTS idx_sections_project ON sections(project_gid);
CREATE INDEX IF NOT EXISTS idx_task_memberships_project ON task_memberships(project_gid);

-- FTS5: per-entity search on individual tasks/subtasks.
-- External content mode keeps index in sync via triggers.
CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(
    name, notes, assignee_name, project_name, section_name, tags,
    content='tasks',
    content_rowid='rowid',
    tokenize='porter unicode61'
);

-- FTS5: holistic search combining top-level task + subtasks + comments.
-- Standalone table (not external content) because each row aggregates
-- text from multiple source rows across tables.
-- task_gid is UNINDEXED so it's stored but not searched.
CREATE VIRTUAL TABLE IF NOT EXISTS task_search_fts USING fts5(
    task_name, task_notes, subtask_text, comment_text,
    assignee_name, project_name,
    task_gid UNINDEXED,
    tokenize='porter unicode61'
);

"""

_FTS_TRIGGERS_SQL = """\

CREATE TRIGGER IF NOT EXISTS tasks_ai AFTER INSERT ON tasks BEGIN
    INSERT INTO tasks_fts(rowid, name, notes, assignee_name, project_name, section_name, tags)
    VALUES (new.rowid, new.name, new.notes, new.assignee_name, new.project_name, new.section_name, new.tags);
END;

CREATE TRIGGER IF NOT EXISTS tasks_ad AFTER DELETE ON tasks BEGIN
    INSERT INTO tasks_fts(tasks_fts, rowid, name, notes, assignee_name, project_name, section_name, tags)
    VALUES ('delete', old.rowid, old.name, old.notes, old.assignee_name, old.project_name, old.section_name, old.tags);
END;

CREATE TRIGGER IF NOT EXISTS tasks_au AFTER UPDATE ON tasks BEGIN
    INSERT INTO tasks_fts(tasks_fts, rowid, name, notes, assignee_name, project_name, section_name, tags)
    VALUES ('delete', old.rowid, old.name, old.notes, old.assignee_name, old.project_name, old.section_name, old.tags);
    INSERT INTO tasks_fts(rowid, name, notes, assignee_name, project_name, section_name, tags)
    VALUES (new.rowid, new.name, new.notes, new.assignee_name, new.project_name, new.section_name, new.tags);
END;

"""


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables, indexes, FTS virtual tables, and triggers."""
    conn.executescript(_SCHEMA_SQL)
    conn.executescript(_FTS_TRIGGERS_SQL)
    conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )
    conn.commit()


def get_metadata(conn: sqlite3.Connection, key: str) -> str | None:
    """Get a metadata value by key."""
    row = conn.execute(
        "SELECT value FROM metadata WHERE key = ?", (key,)
    ).fetchone()
    return row[0] if row else None


def set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Set a metadata value."""
    conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def get_schema_version(conn: sqlite3.Connection) -> int | None:
    """Return the current schema version, or None if metadata table doesn't exist."""
    try:
        row = conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return int(row[0]) if row else None


def migrate_schema(conn: sqlite3.Connection) -> None:
    """Create or migrate the database schema to the latest version."""
    version = get_schema_version(conn)
    if version is None:
        create_schema(conn)
        return
    # Re-apply current schema to pick up any new tables/indexes/triggers
    # added within the same version (all DDL uses IF NOT EXISTS).
    create_schema(conn)
    # Future migrations:
    # if version < 2:
    #     _migrate_v1_to_v2(conn)


def rebuild_task_search_fts(conn: sqlite3.Connection) -> int:
    """Rebuild the holistic task_search_fts table from scratch.

    Aggregates each top-level task with all descendant subtask text and
    all comments (including comments on subtasks) into a single FTS document.
    Uses a recursive CTE to map every task to its top-level root.

    Must be called after bulk import or when subtasks/stories change.
    Returns the number of task documents indexed.
    """
    conn.execute("DELETE FROM task_search_fts")

    conn.execute("""\
        WITH RECURSIVE task_roots(gid, root_gid) AS (
            -- Base: top-level tasks are their own root
            SELECT gid, gid FROM tasks WHERE parent_gid IS NULL
          UNION ALL
            -- Recursive: children inherit their parent's root
            SELECT child.gid, tr.root_gid
            FROM tasks child
            JOIN task_roots tr ON child.parent_gid = tr.gid
        )
        INSERT INTO task_search_fts(
            task_name, task_notes, subtask_text, comment_text,
            assignee_name, project_name, task_gid
        )
        SELECT
            t.name,
            t.notes,
            COALESCE(sub_agg.subtask_text, ''),
            COALESCE(comment_agg.comment_text, ''),
            t.assignee_name,
            t.project_name,
            t.gid
        FROM tasks t
        LEFT JOIN (
            -- Subtask text: concatenate name+notes of all descendants
            SELECT
                tr.root_gid,
                GROUP_CONCAT(sub.name || ' ' || sub.notes, X'0A') AS subtask_text
            FROM task_roots tr
            JOIN tasks sub ON sub.gid = tr.gid
            WHERE tr.gid != tr.root_gid
            GROUP BY tr.root_gid
        ) sub_agg ON sub_agg.root_gid = t.gid
        LEFT JOIN (
            -- Comment text: concatenate comments on root + all descendants
            SELECT
                tr.root_gid,
                GROUP_CONCAT(s.text, X'0A') AS comment_text
            FROM stories s
            JOIN task_roots tr ON tr.gid = s.task_gid
            WHERE s.resource_subtype = 'comment'
            GROUP BY tr.root_gid
        ) comment_agg ON comment_agg.root_gid = t.gid
        WHERE t.parent_gid IS NULL
    """)

    row = conn.execute("SELECT COUNT(*) FROM task_search_fts").fetchone()
    conn.commit()
    return row[0]

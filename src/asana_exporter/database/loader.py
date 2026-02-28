"""Load exported Asana JSON files into SQLite."""

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from asana_exporter.utils import LOG


@dataclass
class ImportStats:
    """Mutable counters for an import run."""

    teams: int = 0
    projects: int = 0
    tasks: int = 0
    subtasks: int = 0
    stories: int = 0
    attachments: int = 0
    skipped_projects: int = 0

    def __str__(self) -> str:
        parts = []
        for name in (
            "teams",
            "projects",
            "tasks",
            "subtasks",
            "stories",
            "attachments",
            "skipped_projects",
        ):
            val = getattr(self, name)
            if val:
                parts.append(f"{name}={val}")
        return ", ".join(parts) if parts else "nothing imported"


def _file_hash(path: Path) -> str:
    """SHA-256 hex digest of a file's contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _should_reimport(conn: sqlite3.Connection, source_path: str, source_hash: str) -> bool:
    """Return True if *source_path* is new or has changed since last import."""
    row = conn.execute(
        "SELECT source_hash FROM sync_state WHERE source_path = ?",
        (source_path,),
    ).fetchone()
    if row is None:
        return True
    return row[0] != source_hash


def _update_sync_state(conn: sqlite3.Connection, source_path: str, source_hash: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO sync_state (source_path, source_hash, last_import_at) "
        "VALUES (?, ?, ?)",
        (source_path, source_hash, int(time.time())),
    )


def _upsert_user(conn: sqlite3.Connection, user: dict | None) -> None:
    """INSERT OR IGNORE a user from inline Asana data (e.g. assignee dict)."""
    if not user or not user.get("gid"):
        return
    conn.execute(
        "INSERT OR IGNORE INTO users (gid, name, email) VALUES (?, ?, ?)",
        (user["gid"], user.get("name", ""), user.get("email", "")),
    )


def _safe_get(data: dict, *keys, default=None):
    """Safely traverse nested dicts, returning *default* on any miss."""
    obj = data
    for k in keys:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(k)
        if obj is None:
            return default
    return obj


# ── Per-entity importers ────────────────────────────────────────────────


def _import_task(
    conn: sqlite3.Connection,
    task_data: dict,
    team: dict,
    depth: int = 0,
    parent_gid: str | None = None,
) -> None:
    """Insert or replace a single task/subtask row and its memberships."""
    gid = task_data["gid"]

    # Extract users that appear inline
    _upsert_user(conn, task_data.get("assignee"))
    _upsert_user(conn, task_data.get("created_by"))
    _upsert_user(conn, task_data.get("completed_by"))

    # Primary membership for denormalized columns (pick first membership
    # whose project exists in the DB to avoid FK violations from cross-team
    # project references)
    memberships = task_data.get("memberships") or []
    first_project = {}
    first_section = {}
    for m in memberships:
        proj = m.get("project") or {}
        if proj.get("gid"):
            exists = conn.execute(
                "SELECT 1 FROM projects WHERE gid = ?",
                (proj["gid"],),
            ).fetchone()
            if exists:
                first_project = proj
                first_section = m.get("section") or {}
                break

    # Sections - upsert every section we see (skip if project not in DB)
    for m in memberships:
        sec = m.get("section") or {}
        proj = m.get("project") or {}
        if sec.get("gid") and proj.get("gid"):
            # Only insert if the project already exists (FK constraint)
            exists = conn.execute(
                "SELECT 1 FROM projects WHERE gid = ?",
                (proj["gid"],),
            ).fetchone()
            if exists:
                conn.execute(
                    "INSERT OR IGNORE INTO sections (gid, project_gid, name) VALUES (?, ?, ?)",
                    (sec["gid"], proj["gid"], sec.get("name", "")),
                )

    # Tags → JSON list of name strings
    tags_raw = task_data.get("tags") or []
    tags_json = json.dumps([t["name"] for t in tags_raw if "name" in t])

    # JSON blob columns
    custom_fields = json.dumps(task_data.get("custom_fields") or [])
    dependencies = json.dumps(task_data.get("dependencies") or [])
    dependents = json.dumps(task_data.get("dependents") or [])
    followers_raw = task_data.get("followers") or []
    followers = json.dumps([f["gid"] for f in followers_raw if "gid" in f])

    conn.execute(
        """INSERT OR REPLACE INTO tasks (
            gid, name, notes, html_notes, resource_subtype,
            parent_gid, depth,
            project_gid, project_name, team_gid, team_name,
            section_gid, section_name,
            assignee_gid, assignee_name,
            completed, completed_at, completed_by_gid,
            created_at, created_by_gid, modified_at,
            due_on, due_at, start_on, start_at,
            num_likes, permalink_url,
            tags, custom_fields, dependencies, dependents, followers,
            raw_json
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?, ?,
            ?
        )""",
        (
            gid,
            task_data.get("name", ""),
            task_data.get("notes", ""),
            task_data.get("html_notes", ""),
            task_data.get("resource_subtype", "default_task"),
            parent_gid,
            depth,
            first_project.get("gid"),
            first_project.get("name", ""),
            team.get("gid"),
            team.get("name", ""),
            first_section.get("gid"),
            first_section.get("name", ""),
            _safe_get(task_data, "assignee", "gid", default=None),
            _safe_get(task_data, "assignee", "name"),
            int(bool(task_data.get("completed"))),
            task_data.get("completed_at"),
            _safe_get(task_data, "completed_by", "gid", default=None),
            task_data.get("created_at"),
            _safe_get(task_data, "created_by", "gid", default=None),
            task_data.get("modified_at"),
            task_data.get("due_on"),
            task_data.get("due_at"),
            task_data.get("start_on"),
            task_data.get("start_at"),
            task_data.get("num_likes", 0),
            task_data.get("permalink_url"),
            tags_json,
            custom_fields,
            dependencies,
            dependents,
            followers,
            json.dumps(task_data),
        ),
    )

    # Task memberships (multi-project support; skip if project not in DB)
    for m in memberships:
        proj = m.get("project") or {}
        sec = m.get("section") or {}
        if proj.get("gid"):
            exists = conn.execute(
                "SELECT 1 FROM projects WHERE gid = ?",
                (proj["gid"],),
            ).fetchone()
            if exists:
                conn.execute(
                    "INSERT OR REPLACE INTO task_memberships "
                    "(task_gid, project_gid, project_name, section_gid, section_name) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        gid,
                        proj["gid"],
                        proj.get("name", ""),
                        sec.get("gid"),
                        sec.get("name", ""),
                    ),
                )


def _import_stories(conn: sqlite3.Connection, stories_path: Path, task_gid: str) -> int:
    """Import stories (comments + events) for a task. Returns count."""
    if not stories_path.exists():
        return 0
    stories = json.loads(stories_path.read_text())

    for s in stories:
        _upsert_user(conn, s.get("created_by"))
        conn.execute(
            """INSERT OR REPLACE INTO stories (
                gid, task_gid, created_at, created_by_gid, created_by_name,
                resource_subtype, type, text, html_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                s["gid"],
                task_gid,
                s.get("created_at"),
                _safe_get(s, "created_by", "gid", default=None),
                _safe_get(s, "created_by", "name"),
                s.get("resource_subtype", ""),
                s.get("type", ""),
                s.get("text", ""),
                s.get("html_text", ""),
            ),
        )
    return len(stories)


def _import_attachments(conn: sqlite3.Connection, attachments_dir: Path, task_gid: str) -> int:
    """Import attachment metadata for a task. Returns count."""
    if not attachments_dir.is_dir():
        return 0

    count = 0
    for att_path in attachments_dir.iterdir():
        # Skip download files (binary blobs)
        if "_download" in att_path.name:
            continue
        if not att_path.is_file():
            continue
        try:
            a = json.loads(att_path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError):
            LOG.debug(f"skipping non-JSON attachment file: {att_path}")
            continue

        if "gid" not in a:
            continue

        _upsert_user(conn, a.get("created_by"))
        conn.execute(
            """INSERT OR REPLACE INTO attachments (
                gid, task_gid, name, file_name, host, size,
                created_at, download_url, permanent_url,
                uploaded_by_gid, uploaded_by_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                a["gid"],
                task_gid,
                a.get("name", ""),
                a.get("file_name") or a.get("name", ""),
                a.get("host", ""),
                a.get("size"),
                a.get("created_at"),
                a.get("download_url"),
                a.get("permanent_url"),
                _safe_get(a, "created_by", "gid", default=None),
                _safe_get(a, "created_by", "name"),
            ),
        )
        count += 1
    return count


# ── Task-tree walker ────────────────────────────────────────────────────


def _import_task_tree(
    conn: sqlite3.Connection,
    task_dir: Path,
    json_filename: str,
    team: dict,
    depth: int = 0,
    parent_gid: str | None = None,
    stats: ImportStats | None = None,
) -> None:
    """Import a task and all its subtasks recursively.

    *task_dir* is e.g. ``projects/{pid}/tasks/{tid}`` and must contain
    *json_filename* (``task.json`` or ``subtask.json``).
    """
    task_json_path = task_dir / json_filename
    if not task_json_path.exists():
        LOG.debug(f"no {json_filename} in {task_dir} - skipping")
        return

    task_data = json.loads(task_json_path.read_text())

    gid = task_data["gid"]
    _import_task(conn, task_data, team, depth=depth, parent_gid=parent_gid)
    if stats:
        if depth == 0:
            stats.tasks += 1
        else:
            stats.subtasks += 1

    # Stories
    n = _import_stories(conn, task_dir / "stories.json", gid)
    if stats:
        stats.stories += n

    # Attachments (individual JSON files in attachments/ dir)
    n = _import_attachments(conn, task_dir / "attachments", gid)
    if stats:
        stats.attachments += n

    # Subtasks (recursive)
    subtasks_dir = task_dir / "subtasks"
    if subtasks_dir.is_dir():
        for sub_dir in subtasks_dir.iterdir():
            if sub_dir.is_dir():
                _import_task_tree(
                    conn,
                    sub_dir,
                    "subtask.json",
                    team,
                    depth=depth + 1,
                    parent_gid=gid,
                    stats=stats,
                )


# ── Project / team / top-level importers ────────────────────────────────


def _upsert_project(conn: sqlite3.Connection, project_data: dict, team: dict) -> None:
    """Insert or update a project record."""
    conn.execute(
        """INSERT OR REPLACE INTO projects (
            gid, name, team_gid, archived, description,
            owner_gid, created_at, modified_at, due_date, permalink_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            project_data["gid"],
            project_data.get("name", ""),
            team.get("gid"),
            int(bool(project_data.get("archived"))),
            project_data.get("description") or project_data.get("notes", ""),
            _safe_get(project_data, "owner", "gid", default=None),
            project_data.get("created_at"),
            project_data.get("modified_at"),
            project_data.get("due_date") or project_data.get("due_on"),
            project_data.get("permalink_url"),
        ),
    )


def _import_project_tasks(
    conn: sqlite3.Connection,
    project_data: dict,
    project_dir: Path,
    team: dict,
    force: bool,
    stats: ImportStats,
) -> None:
    """Import all tasks for a single project (project record must exist)."""
    tasks_json = project_dir / "tasks.json"
    if not tasks_json.exists():
        LOG.debug(f"no tasks.json in {project_dir} - skipping project")
        return

    # Hash-based change detection (per-project granularity)
    source_hash = _file_hash(tasks_json)
    rel_path = str(tasks_json)  # absolute path as key is fine
    if not force and not _should_reimport(conn, rel_path, source_hash):
        LOG.debug(f"project '{project_data.get('name')}' unchanged - skipping")
        stats.skipped_projects += 1
        return

    stats.projects += 1

    # Walk individual task directories
    tasks_dir = project_dir / "tasks"
    if not tasks_dir.is_dir():
        _update_sync_state(conn, rel_path, source_hash)
        conn.commit()
        return

    for task_dir in tasks_dir.iterdir():
        if task_dir.is_dir():
            _import_task_tree(
                conn, task_dir, "task.json", team, depth=0, parent_gid=None, stats=stats
            )

    _update_sync_state(conn, rel_path, source_hash)
    conn.commit()


def _import_team(
    conn: sqlite3.Connection, team_data: dict, team_dir: Path, force: bool, stats: ImportStats
) -> None:
    """Import all projects for a single team."""
    conn.execute(
        "INSERT OR REPLACE INTO teams (gid, name) VALUES (?, ?)",
        (team_data["gid"], team_data.get("name", "")),
    )
    stats.teams += 1

    projects_json = team_dir / "projects.json"
    if not projects_json.exists():
        LOG.debug(f"no projects.json in {team_dir}")
        conn.commit()
        return

    projects = json.loads(projects_json.read_text())

    # Pass 1: insert all project records (so FK references work for
    # tasks that belong to multiple projects)
    projects_dir = team_dir / "projects"
    for p in projects:
        _upsert_project(conn, p, team_data)
    conn.commit()

    # Pass 2: import tasks per project
    for p in projects:
        project_dir = projects_dir / p["gid"]
        if project_dir.is_dir():
            _import_project_tasks(conn, p, project_dir, team_data, force, stats)

    conn.commit()


# ── Public entry point ──────────────────────────────────────────────────


def import_export_dir(
    conn: sqlite3.Connection, export_path: str, force: bool = False
) -> ImportStats:
    """Walk an asana-exporter JSON tree and load it into SQLite.

    Args:
        conn: SQLite connection with schema already created.
        export_path: Root of the JSON export (contains teams.json).
        force: Re-import all projects regardless of hash.

    Returns:
        ImportStats with counts of imported entities.
    """
    stats = ImportStats()
    root = Path(export_path)

    teams_json = root / "teams.json"
    if not teams_json.exists():
        LOG.info(f"no teams.json found in {export_path} - nothing to import")
        return stats

    teams = json.loads(teams_json.read_text())

    teams_dir = root / "teams"
    for t in teams:
        team_dir = teams_dir / t["gid"]
        if team_dir.is_dir():
            _import_team(conn, t, team_dir, force, stats)

    return stats

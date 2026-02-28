"""Shared test fixtures."""

import sqlite3
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from asana_exporter.database.loader import import_export_dir
from asana_exporter.database.schema import create_schema, rebuild_task_search_fts
from tests.factories import build_export_tree, make_story, make_task


@pytest.fixture
def db() -> Generator[sqlite3.Connection]:
    """In-memory SQLite connection with schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    create_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def populated_db(db: sqlite3.Connection, tmp_path: Path) -> sqlite3.Connection:
    """In-memory DB populated with realistic Asana data for query testing."""
    teams = [
        {"gid": "team1", "name": "Engineering"},
        {"gid": "team2", "name": "Design"},
    ]
    projects: dict[str, list[dict[str, Any]]] = {
        "team1": [
            {"gid": "proj1", "name": "Backend API"},
            {"gid": "proj2", "name": "Frontend App"},
        ],
        "team2": [
            {"gid": "proj3", "name": "Brand Guide"},
        ],
    }

    t1 = make_task(
        "t1", "Fix login bug",
        project_gid="proj1", project_name="Backend API",
        section_gid="sec1", section_name="To Do",
        assignee={"gid": "user1", "name": "Alice", "email": "alice@example.com"},
        notes="Login fails on mobile",
        due_on="2024-02-01",
    )
    t2 = make_task(
        "t2", "Add API rate limiting",
        project_gid="proj1", project_name="Backend API",
        section_gid="sec2", section_name="In Progress",
        assignee={"gid": "user2", "name": "Bob", "email": "bob@example.com"},
    )
    t3 = make_task(
        "t3", "Write API docs",
        project_gid="proj1", project_name="Backend API",
        section_gid="sec3", section_name="Done",
        assignee={"gid": "user1", "name": "Alice", "email": "alice@example.com"},
        completed=True,
    )
    t4 = make_task(
        "t4", "Redesign dashboard",
        project_gid="proj2", project_name="Frontend App",
        section_gid="sec1", section_name="To Do",
        assignee={"gid": "user2", "name": "Bob", "email": "bob@example.com"},
        notes="Use new component library",
    )
    t5 = make_task(
        "t5", "Update color palette",
        project_gid="proj3", project_name="Brand Guide",
        section_gid="sec4", section_name="Research",
        assignee={"gid": "user1", "name": "Alice", "email": "alice@example.com"},
    )
    t6 = make_task(
        "t6", "Deploy v2.0",
        project_gid="proj1", project_name="Backend API",
        section_gid="sec2", section_name="In Progress",
        due_on="2024-03-01",
    )

    subtask = make_task(
        "st1", "Reproduce on Android",
        project_gid="proj1", project_name="Backend API",
        section_gid="sec1", section_name="To Do",
        assignee={"gid": "user2", "name": "Bob", "email": "bob@example.com"},
    )

    comment = make_story("s1", "Found the root cause - session token expires")

    tasks_map: dict[tuple[str, str], list[dict[str, Any]]] = {
        ("team1", "proj1"): [t1, t2, t3, t6],
        ("team1", "proj2"): [t4],
        ("team2", "proj3"): [t5],
    }

    build_export_tree(
        str(tmp_path),
        teams=teams,
        projects=projects,
        tasks=tasks_map,
        stories={"t1": [comment]},
        subtasks={"t1": [(subtask, [])]},
    )

    import_export_dir(db, str(tmp_path))
    rebuild_task_search_fts(db)
    db.row_factory = sqlite3.Row
    return db

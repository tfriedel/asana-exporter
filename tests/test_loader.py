"""Tests for asana_exporter.database.loader."""

import json
import sqlite3
from pathlib import Path

import pytest

from asana_exporter.database.loader import import_export_dir
from asana_exporter.database.schema import create_schema, rebuild_task_search_fts

# ── Fixture helpers ─────────────────────────────────────────────────────


def _write_json(path, data):
    p = Path(path)
    p.parent.mkdir(exist_ok=True, parents=True)
    p.write_text(json.dumps(data))


def _make_task(
    gid,
    name,
    project_gid="proj1",
    project_name="Project One",
    section_gid="sec1",
    section_name="To Do",
    assignee=None,
    **extra,
):
    """Build a realistic Asana task dict."""
    task = {
        "gid": gid,
        "name": name,
        "notes": extra.pop("notes", ""),
        "html_notes": extra.pop("html_notes", ""),
        "resource_subtype": "default_task",
        "assignee": assignee,
        "memberships": [
            {
                "project": {"gid": project_gid, "name": project_name},
                "section": {"gid": section_gid, "name": section_name},
            }
        ],
        "completed": extra.pop("completed", False),
        "completed_at": None,
        "completed_by": None,
        "created_at": "2024-01-15T10:00:00.000Z",
        "created_by": {"gid": "user1", "name": "Alice"},
        "modified_at": "2024-01-16T12:00:00.000Z",
        "due_on": None,
        "due_at": None,
        "start_on": None,
        "start_at": None,
        "num_likes": 0,
        "permalink_url": f"https://app.asana.com/0/{project_gid}/{gid}",
        "tags": extra.pop("tags", []),
        "custom_fields": extra.pop("custom_fields", []),
        "dependencies": [],
        "dependents": [],
        "followers": [{"gid": "user1", "name": "Alice"}],
    }
    task.update(extra)
    return task


def _make_story(gid, text, resource_subtype="comment"):
    return {
        "gid": gid,
        "text": text,
        "html_text": f"<p>{text}</p>",
        "created_at": "2024-01-15T11:00:00.000Z",
        "created_by": {"gid": "user2", "name": "Bob"},
        "resource_subtype": resource_subtype,
        "type": resource_subtype,
    }


def _make_attachment(gid, name):
    return {
        "gid": gid,
        "name": name,
        "file_name": name,
        "host": "asana",
        "size": 1024,
        "created_at": "2024-01-15T12:00:00.000Z",
        "download_url": f"https://example.com/dl/{gid}",
        "permanent_url": f"https://example.com/perm/{gid}",
        "created_by": {"gid": "user1", "name": "Alice"},
    }


def _build_export_tree(
    root, teams=None, projects=None, tasks=None, stories=None, subtasks=None, attachments=None
):
    """Build a minimal export directory tree.

    Args:
        root: Temp directory root.
        teams: List of team dicts.
        projects: Dict mapping team_gid -> list of project dicts.
        tasks: Dict mapping (team_gid, project_gid) -> list of task dicts.
        stories: Dict mapping task_gid -> list of story dicts.
        subtasks: Dict mapping task_gid -> list of (subtask_dict, subtask_stories) pairs.
        attachments: Dict mapping task_gid -> list of attachment dicts.
    """
    teams = teams or [{"gid": "team1", "name": "Engineering"}]
    projects = projects or {"team1": [{"gid": "proj1", "name": "Project One"}]}
    tasks = tasks or {}
    stories = stories or {}
    subtasks = subtasks or {}
    attachments = attachments or {}

    root_path = Path(root)
    _write_json(root_path / "teams.json", teams)

    for team in teams:
        tgid = team["gid"]
        team_dir = root_path / "teams" / tgid
        _write_json(team_dir / "projects.json", projects.get(tgid, []))

        for proj in projects.get(tgid, []):
            pgid = proj["gid"]
            proj_dir = team_dir / "projects" / pgid

            # tasks.json (summary list)
            task_list = tasks.get((tgid, pgid), [])
            _write_json(
                proj_dir / "tasks.json",
                [{"gid": t["gid"], "name": t["name"]} for t in task_list],
            )

            # Individual task directories
            for task in task_list:
                task_dir = proj_dir / "tasks" / task["gid"]
                _write_json(task_dir / "task.json", task)

                # Stories
                if task["gid"] in stories:
                    _write_json(task_dir / "stories.json", stories[task["gid"]])

                # Attachments (individual files in attachments/ dir)
                if task["gid"] in attachments:
                    att_dir = task_dir / "attachments"
                    for att in attachments[task["gid"]]:
                        _write_json(att_dir / att["gid"], att)

                # Subtasks
                if task["gid"] in subtasks:
                    for sub, sub_stories in subtasks[task["gid"]]:
                        sub_dir = task_dir / "subtasks" / sub["gid"]
                        _write_json(sub_dir / "subtask.json", sub)
                        if sub_stories:
                            _write_json(sub_dir / "stories.json", sub_stories)


@pytest.fixture
def db():
    """In-memory SQLite connection with schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    create_schema(conn)
    yield conn
    conn.close()


# ── Tests ───────────────────────────────────────────────────────────────


class TestBasicImport:
    def test_empty_export_dir(self, db, tmp_path):
        """No teams.json → nothing imported."""
        stats = import_export_dir(db, str(tmp_path))
        assert stats.teams == 0
        assert stats.tasks == 0

    def test_single_task(self, db, tmp_path):
        """Import one team, one project, one task."""
        task = _make_task("t1", "Fix the bug")
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        stats = import_export_dir(db, str(tmp_path))
        assert stats.teams == 1
        assert stats.projects == 1
        assert stats.tasks == 1
        assert stats.subtasks == 0

        # Verify task in DB
        row = db.execute(
            "SELECT name, project_name, team_name FROM tasks WHERE gid = 't1'"
        ).fetchone()
        assert row == ("Fix the bug", "Project One", "Engineering")

    def test_task_with_stories(self, db, tmp_path):
        """Stories are imported and linked to their task."""
        task = _make_task("t1", "Task with comments")
        story = _make_story("s1", "Looks good to me!")
        _build_export_tree(
            str(tmp_path), tasks={("team1", "proj1"): [task]}, stories={"t1": [story]}
        )

        stats = import_export_dir(db, str(tmp_path))
        assert stats.stories == 1

        row = db.execute("SELECT text, created_by_name FROM stories WHERE gid = 's1'").fetchone()
        assert row == ("Looks good to me!", "Bob")

    def test_task_with_attachments(self, db, tmp_path):
        """Attachment metadata is imported."""
        task = _make_task("t1", "Task with file")
        att = _make_attachment("a1", "screenshot.png")
        _build_export_tree(
            str(tmp_path), tasks={("team1", "proj1"): [task]}, attachments={"t1": [att]}
        )

        stats = import_export_dir(db, str(tmp_path))
        assert stats.attachments == 1

        row = db.execute("SELECT name, host FROM attachments WHERE gid = 'a1'").fetchone()
        assert row == ("screenshot.png", "asana")

    def test_task_with_subtasks(self, db, tmp_path):
        """Subtasks get parent_gid and depth set correctly."""
        task = _make_task("t1", "Parent task")
        subtask = _make_task("st1", "Child task", assignee={"gid": "user3", "name": "Charlie"})
        sub_story = _make_story("ss1", "Subtask comment")

        _build_export_tree(
            str(tmp_path),
            tasks={("team1", "proj1"): [task]},
            subtasks={"t1": [(subtask, [sub_story])]},
        )

        stats = import_export_dir(db, str(tmp_path))
        assert stats.tasks == 1
        assert stats.subtasks == 1
        assert stats.stories == 1  # subtask story

        row = db.execute(
            "SELECT parent_gid, depth, assignee_name FROM tasks WHERE gid = 'st1'"
        ).fetchone()
        assert row == ("t1", 1, "Charlie")


class TestIncrementalImport:
    def test_skip_unchanged_project(self, db, tmp_path):
        """Second import skips unchanged projects."""
        task = _make_task("t1", "Stable task")
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        stats1 = import_export_dir(db, str(tmp_path))
        assert stats1.projects == 1
        assert stats1.skipped_projects == 0

        stats2 = import_export_dir(db, str(tmp_path))
        assert stats2.projects == 0
        assert stats2.skipped_projects == 1

    def test_reimport_changed_project(self, db, tmp_path):
        """Changing tasks.json causes reimport."""
        task = _make_task("t1", "Original name")
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Modify tasks.json to trigger reimport
        tasks_json = tmp_path / "teams" / "team1" / "projects" / "proj1" / "tasks.json"
        tasks_json.write_text(json.dumps([{"gid": "t1", "name": "Updated name"}]))

        stats = import_export_dir(db, str(tmp_path))
        assert stats.projects == 1
        assert stats.skipped_projects == 0

    def test_force_reimport(self, db, tmp_path):
        """--force bypasses hash check."""
        task = _make_task("t1", "Task")
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))
        stats = import_export_dir(db, str(tmp_path), force=True)
        assert stats.projects == 1
        assert stats.skipped_projects == 0


class TestUserExtraction:
    def test_users_from_task_fields(self, db, tmp_path):
        """Users are extracted from assignee, created_by, etc."""
        task = _make_task("t1", "Task", assignee={"gid": "u1", "name": "Eve"})
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        users = db.execute("SELECT gid, name FROM users ORDER BY gid").fetchall()
        # user1 (Alice, from created_by) and u1 (Eve, from assignee)
        gids = [u[0] for u in users]
        assert "u1" in gids
        assert "user1" in gids


class TestFTSIntegration:
    def test_per_entity_fts(self, db, tmp_path):
        """FTS triggers populate tasks_fts on insert."""
        task = _make_task(
            "t1",
            "Deploy the widget",
            notes="We need to deploy ASAP",
            tags=[{"gid": "tag1", "name": "urgent"}],
        )
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Search by name
        rows = db.execute("SELECT name FROM tasks_fts WHERE tasks_fts MATCH 'widget'").fetchall()
        assert len(rows) == 1

        # Search by tag
        rows = db.execute("SELECT name FROM tasks_fts WHERE tasks_fts MATCH 'urgent'").fetchall()
        assert len(rows) == 1

    def test_holistic_fts_rebuild(self, db, tmp_path):
        """rebuild_task_search_fts aggregates subtask and comment text."""
        task = _make_task("t1", "Parent")
        subtask = _make_task("st1", "Investigate memory leak")
        comment = _make_story("s1", "Found the root cause in allocator")

        _build_export_tree(
            str(tmp_path),
            tasks={("team1", "proj1"): [task]},
            subtasks={"t1": [(subtask, [comment])]},
        )

        import_export_dir(db, str(tmp_path))
        count = rebuild_task_search_fts(db)
        assert count == 1  # one top-level task

        # Search finds parent task via subtask text
        rows = db.execute(
            "SELECT task_gid FROM task_search_fts WHERE task_search_fts MATCH 'memory'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "t1"

        # Search finds parent task via comment text
        rows = db.execute(
            "SELECT task_gid FROM task_search_fts WHERE task_search_fts MATCH 'allocator'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "t1"


class TestTaskMemberships:
    def test_memberships_stored(self, db, tmp_path):
        """Task memberships are stored for projects that exist in the DB."""
        task = _make_task("t1", "Multi-project task")
        # Add a second membership for a project NOT in our export
        task["memberships"].append(
            {
                "project": {"gid": "proj2", "name": "Project Two"},
                "section": {"gid": "sec2", "name": "In Progress"},
            }
        )
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Only proj1 membership is stored (proj2 not in projects table)
        rows = db.execute(
            "SELECT project_gid, section_name FROM task_memberships "
            "WHERE task_gid = 't1' ORDER BY project_gid"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("proj1", "To Do")

    def test_memberships_for_multiple_exported_projects(self, db, tmp_path):
        """Both memberships stored when both projects are in the export."""
        task = _make_task("t1", "Multi-project task")
        task["memberships"].append(
            {
                "project": {"gid": "proj2", "name": "Project Two"},
                "section": {"gid": "sec2", "name": "In Progress"},
            }
        )
        _build_export_tree(
            str(tmp_path),
            projects={
                "team1": [
                    {"gid": "proj1", "name": "Project One"},
                    {"gid": "proj2", "name": "Project Two"},
                ]
            },
            tasks={
                ("team1", "proj1"): [task],
                ("team1", "proj2"): [],  # proj2 exists but has no tasks of its own
            },
        )

        import_export_dir(db, str(tmp_path))

        rows = db.execute(
            "SELECT project_gid, section_name FROM task_memberships "
            "WHERE task_gid = 't1' ORDER BY project_gid"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("proj1", "To Do")
        assert rows[1] == ("proj2", "In Progress")


class TestSections:
    def test_sections_extracted(self, db, tmp_path):
        """Sections are extracted from task memberships."""
        task = _make_task("t1", "Task in section")
        _build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        row = db.execute("SELECT name, project_gid FROM sections WHERE gid = 'sec1'").fetchone()
        assert row == ("To Do", "proj1")

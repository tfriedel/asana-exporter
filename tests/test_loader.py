"""Tests for asana_exporter.database.loader."""

import json
import sqlite3
from pathlib import Path

from asana_exporter.database.loader import import_export_dir
from asana_exporter.database.schema import rebuild_task_search_fts
from tests.factories import build_export_tree, make_attachment, make_story, make_task

# ── Tests ───────────────────────────────────────────────────────────────


class TestBasicImport:
    def test_empty_export_dir(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """No teams.json -> nothing imported."""
        stats = import_export_dir(db, str(tmp_path))
        assert stats.teams == 0
        assert stats.tasks == 0

    def test_single_task(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Import one team, one project, one task."""
        task = make_task("t1", "Fix the bug")
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

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

    def test_task_with_stories(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Stories are imported and linked to their task."""
        task = make_task("t1", "Task with comments")
        story = make_story("s1", "Looks good to me!")
        build_export_tree(
            str(tmp_path), tasks={("team1", "proj1"): [task]}, stories={"t1": [story]}
        )

        stats = import_export_dir(db, str(tmp_path))
        assert stats.stories == 1

        row = db.execute("SELECT text, created_by_name FROM stories WHERE gid = 's1'").fetchone()
        assert row == ("Looks good to me!", "Bob")

    def test_task_with_attachments(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Attachment metadata is imported."""
        task = make_task("t1", "Task with file")
        att = make_attachment("a1", "screenshot.png")
        build_export_tree(
            str(tmp_path), tasks={("team1", "proj1"): [task]}, attachments={"t1": [att]}
        )

        stats = import_export_dir(db, str(tmp_path))
        assert stats.attachments == 1

        row = db.execute("SELECT name, host FROM attachments WHERE gid = 'a1'").fetchone()
        assert row == ("screenshot.png", "asana")

    def test_task_with_subtasks(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Subtasks get parent_gid and depth set correctly."""
        task = make_task("t1", "Parent task")
        subtask = make_task("st1", "Child task", assignee={"gid": "user3", "name": "Charlie"})
        sub_story = make_story("ss1", "Subtask comment")

        build_export_tree(
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
    def test_skip_unchanged_project(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Second import skips unchanged projects."""
        task = make_task("t1", "Stable task")
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        stats1 = import_export_dir(db, str(tmp_path))
        assert stats1.projects == 1
        assert stats1.skipped_projects == 0

        stats2 = import_export_dir(db, str(tmp_path))
        assert stats2.projects == 0
        assert stats2.skipped_projects == 1

    def test_reimport_changed_project(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Changing tasks.json causes reimport."""
        task = make_task("t1", "Original name")
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Modify tasks.json to trigger reimport
        tasks_json = tmp_path / "teams" / "team1" / "projects" / "proj1" / "tasks.json"
        tasks_json.write_text(json.dumps([{"gid": "t1", "name": "Updated name"}]))

        stats = import_export_dir(db, str(tmp_path))
        assert stats.projects == 1
        assert stats.skipped_projects == 0

    def test_force_reimport(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """--force bypasses hash check."""
        task = make_task("t1", "Task")
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))
        stats = import_export_dir(db, str(tmp_path), force=True)
        assert stats.projects == 1
        assert stats.skipped_projects == 0


class TestUserExtraction:
    def test_users_from_task_fields(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Users are extracted from assignee, created_by, etc."""
        task = make_task("t1", "Task", assignee={"gid": "u1", "name": "Eve"})
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        users = db.execute("SELECT gid, name FROM users ORDER BY gid").fetchall()
        # user1 (Alice, from created_by) and u1 (Eve, from assignee)
        gids = [u[0] for u in users]
        assert "u1" in gids
        assert "user1" in gids


class TestFTSIntegration:
    def test_per_entity_fts(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """FTS triggers populate tasks_fts on insert."""
        task = make_task(
            "t1",
            "Deploy the widget",
            notes="We need to deploy ASAP",
            tags=[{"gid": "tag1", "name": "urgent"}],
        )
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Search by name
        rows = db.execute("SELECT name FROM tasks_fts WHERE tasks_fts MATCH 'widget'").fetchall()
        assert len(rows) == 1

        # Search by tag
        rows = db.execute("SELECT name FROM tasks_fts WHERE tasks_fts MATCH 'urgent'").fetchall()
        assert len(rows) == 1

    def test_holistic_fts_rebuild(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """rebuild_task_search_fts aggregates subtask and comment text."""
        task = make_task("t1", "Parent")
        subtask = make_task("st1", "Investigate memory leak")
        comment = make_story("s1", "Found the root cause in allocator")

        build_export_tree(
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
    def test_memberships_stored(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Task memberships are stored for projects that exist in the DB."""
        task = make_task("t1", "Multi-project task")
        # Add a second membership for a project NOT in our export
        task["memberships"].append(
            {
                "project": {"gid": "proj2", "name": "Project Two"},
                "section": {"gid": "sec2", "name": "In Progress"},
            }
        )
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        # Only proj1 membership is stored (proj2 not in projects table)
        rows = db.execute(
            "SELECT project_gid, section_name FROM task_memberships "
            "WHERE task_gid = 't1' ORDER BY project_gid"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("proj1", "To Do")

    def test_memberships_for_multiple_exported_projects(
        self, db: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Both memberships stored when both projects are in the export."""
        task = make_task("t1", "Multi-project task")
        task["memberships"].append(
            {
                "project": {"gid": "proj2", "name": "Project Two"},
                "section": {"gid": "sec2", "name": "In Progress"},
            }
        )
        build_export_tree(
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
    def test_sections_extracted(self, db: sqlite3.Connection, tmp_path: Path) -> None:
        """Sections are extracted from task memberships."""
        task = make_task("t1", "Task in section")
        build_export_tree(str(tmp_path), tasks={("team1", "proj1"): [task]})

        import_export_dir(db, str(tmp_path))

        row = db.execute("SELECT name, project_gid FROM sections WHERE gid = 'sec1'").fetchone()
        assert row == ("To Do", "proj1")

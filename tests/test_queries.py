"""Tests for asana_exporter.database.queries."""

import sqlite3

from asana_exporter.database.queries import (
    _prepare_fts_query,
    get_project,
    get_projects,
    get_task,
    get_tasks,
    get_user,
    search_objects,
    search_tasks,
)


class TestGetUser:
    def test_get_user_by_gid_returns_user(self, populated_db: sqlite3.Connection) -> None:
        result = get_user(populated_db, user_gid="user1")
        assert result["gid"] == "user1"
        assert result["name"] == "Alice"

    def test_get_user_not_found_returns_error(self, populated_db: sqlite3.Connection) -> None:
        result = get_user(populated_db, user_gid="nonexistent")
        assert "error" in result

    def test_get_user_by_email_returns_user(self, populated_db: sqlite3.Connection) -> None:
        result = get_user(populated_db, email="alice@example.com")
        assert result["gid"] == "user1"
        assert result["name"] == "Alice"


class TestGetProject:
    def test_get_project_returns_project_with_sections(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = get_project(populated_db, project_gid="proj1")
        assert result["name"] == "Backend API"
        assert "sections" in result
        assert len(result["sections"]) >= 2

    def test_get_project_not_found_returns_error(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = get_project(populated_db, project_gid="nonexistent")
        assert "error" in result


class TestGetProjects:
    def test_get_projects_returns_all_projects(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = get_projects(populated_db)
        assert result["total"] == 3
        assert result["count"] == 3

    def test_get_projects_filters_by_team(self, populated_db: sqlite3.Connection) -> None:
        result = get_projects(populated_db, team_gid="team1")
        assert result["total"] == 2
        assert all(p["team_gid"] == "team1" for p in result["results"])

    def test_get_projects_filters_by_archived(self, populated_db: sqlite3.Connection) -> None:
        result = get_projects(populated_db, archived=False)
        assert result["total"] == 3  # None are explicitly archived in our fixture

    def test_get_projects_pagination(self, populated_db: sqlite3.Connection) -> None:
        result = get_projects(populated_db, limit=2, offset=0)
        assert result["count"] == 2
        assert result["total"] == 3
        assert result["has_more"] is True
        assert result["next_offset"] == 2

        result2 = get_projects(populated_db, limit=2, offset=2)
        assert result2["count"] == 1
        assert result2["has_more"] is False


class TestGetTask:
    def test_get_task_returns_task_details(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="t1")
        assert result["name"] == "Fix login bug"
        assert result["assignee_name"] == "Alice"
        assert isinstance(result["tags"], list)
        assert "raw_json" not in result

    def test_get_task_not_found_returns_error(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="nonexistent")
        assert "error" in result

    def test_get_task_includes_num_subtasks(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="t1")
        assert result["num_subtasks"] == 1

    def test_get_task_includes_parent_name(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="st1")
        assert result["parent_name"] == "Fix login bug"

    def test_get_task_includes_subtasks(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="t1", include_subtasks=True)
        assert len(result["subtasks"]) == 1
        assert result["subtasks"][0]["name"] == "Reproduce on Android"

    def test_get_task_includes_stories(self, populated_db: sqlite3.Connection) -> None:
        result = get_task(populated_db, task_gid="t1", include_stories=True)
        assert len(result["stories"]) == 1
        assert "root cause" in result["stories"][0]["text"]


class TestGetTasks:
    def test_get_tasks_by_project(self, populated_db: sqlite3.Connection) -> None:
        result = get_tasks(populated_db, project_gid="proj1")
        assert result["total"] >= 4  # t1, t2, t3, t6 (+ subtask st1)
        assert all(t["project_gid"] == "proj1" for t in result["results"])

    def test_get_tasks_filters_by_completed(self, populated_db: sqlite3.Connection) -> None:
        result = get_tasks(populated_db, project_gid="proj1", completed=True)
        assert result["total"] >= 1
        assert all(t["completed"] for t in result["results"])

    def test_get_tasks_filters_by_assignee(self, populated_db: sqlite3.Connection) -> None:
        result = get_tasks(populated_db, assignee_gid="user1")
        assert result["total"] >= 1
        assert all(t["assignee_gid"] == "user1" for t in result["results"])

    def test_get_tasks_pagination(self, populated_db: sqlite3.Connection) -> None:
        result = get_tasks(populated_db, project_gid="proj1", limit=2, offset=0)
        assert result["count"] == 2
        assert result["has_more"] is True


class TestSearchTasks:
    def test_search_tasks_finds_by_name(self, populated_db: sqlite3.Connection) -> None:
        result = search_tasks(populated_db, query="login")
        assert result["count"] >= 1
        assert any(r["name"] == "Fix login bug" for r in result["results"])

    def test_search_tasks_filters_by_project(self, populated_db: sqlite3.Connection) -> None:
        result = search_tasks(populated_db, query="dashboard", project_gid="proj2")
        assert result["count"] >= 1
        assert all(r["project_gid"] == "proj2" for r in result["results"])

    def test_search_tasks_filters_by_completed(self, populated_db: sqlite3.Connection) -> None:
        result = search_tasks(populated_db, query="API", completed=True)
        assert result["count"] >= 1
        assert all(r["completed"] for r in result["results"])

    def test_search_tasks_finds_by_notes(self, populated_db: sqlite3.Connection) -> None:
        result = search_tasks(populated_db, query="mobile")
        assert result["count"] >= 1
        assert any(r["name"] == "Fix login bug" for r in result["results"])

    def test_search_tasks_holistic_finds_in_comments(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = search_tasks(populated_db, query="root cause", use_holistic=True)
        assert result["count"] >= 1
        # t1 found because its comment mentions "root cause"
        assert any(r["gid"] == "t1" for r in result["results"])

    def test_search_tasks_finds_underscored_terms(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = search_tasks(populated_db, query="user_id")
        assert result["count"] >= 1
        assert any(r["name"] == "Rename user_id to account_id" for r in result["results"])

    def test_holistic_search_snippet_shows_comment_match(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = search_tasks(
            populated_db, query="session token", use_holistic=True
        )
        assert result["count"] >= 1
        match = next(r for r in result["results"] if r["gid"] == "t1")
        # Snippet should come from the comment, not the task name
        assert "session" in match["snippet"].lower() or "token" in match["snippet"].lower()

    def test_holistic_snippet_prefers_most_highlighted(
        self, populated_db: sqlite3.Connection
    ) -> None:
        # "login" matches notes, "session token" matches comment.
        # Comment has more highlighted terms, so should be preferred.
        result = search_tasks(
            populated_db, query="login session token", use_holistic=True
        )
        assert result["count"] >= 1
        match = next(r for r in result["results"] if r["gid"] == "t1")
        assert "[comment]" in match["snippet"]

    def test_search_tasks_empty_query_returns_error(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = search_tasks(populated_db, query="")
        assert "error" in result

    def test_search_tasks_pagination(self, populated_db: sqlite3.Connection) -> None:
        result = search_tasks(populated_db, query="API", limit=1, offset=0)
        assert result["count"] == 1
        assert result["has_more"] is True


class TestSearchObjects:
    def test_search_objects_finds_projects(self, populated_db: sqlite3.Connection) -> None:
        result = search_objects(populated_db, query="Backend", resource_type="project")
        assert result["count"] >= 1
        assert any(r["name"] == "Backend API" for r in result["results"])

    def test_search_objects_finds_users(self, populated_db: sqlite3.Connection) -> None:
        result = search_objects(populated_db, query="Alice", resource_type="user")
        assert result["count"] >= 1
        assert any(r["name"] == "Alice" for r in result["results"])

    def test_search_objects_invalid_type_returns_error(
        self, populated_db: sqlite3.Connection
    ) -> None:
        result = search_objects(populated_db, query="test", resource_type="invalid")
        assert "error" in result


class TestPrepareFtsQuery:
    def test_underscores_split_into_separate_tokens(self) -> None:
        result = _prepare_fts_query("user_id")
        assert result == "user* id"


class TestMcpServer:
    def test_mcp_server_creates_without_error(self) -> None:
        from asana_exporter.mcp.server import mcp_server

        assert mcp_server.name == "asana-archive"

    def test_tool_names_registered(self) -> None:
        from asana_exporter.mcp.server import mcp_server

        tool_names = [tool.name for tool in mcp_server._tool_manager.list_tools()]
        expected = [
            "search_tasks",
            "get_task",
            "get_tasks",
            "get_project",
            "get_projects",
            "get_user",
            "search_objects",
        ]
        for name in expected:
            assert name in tool_names, f"Missing tool: {name}"

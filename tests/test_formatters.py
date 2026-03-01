"""Tests for asana_exporter.mcp.formatters."""

from asana_exporter.mcp.formatters import (
    format_list,
    format_project,
    format_search_objects,
    format_single,
    format_task,
    format_user,
    parse_opt_fields,
)


class TestFormatTaskCompact:
    def test_compact_returns_only_gid_name_permalink(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "notes": "Login fails on mobile",
            "assignee_gid": "u1",
            "assignee_name": "Alice",
            "completed": 0,
            "custom_fields": [],
            "followers": [],
        }
        result = format_task(row, opt_fields=None)
        assert result == {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
        }


class TestFormatProjectCompact:
    def test_compact_returns_only_gid_name_permalink(self) -> None:
        row = {
            "gid": "p1",
            "name": "Backend API",
            "permalink_url": "https://app.asana.com/0/0/p1",
            "team_gid": "team1",
            "archived": 0,
            "description": "Project description",
            "owner_gid": "u1",
        }
        result = format_project(row, opt_fields=None)
        assert result == {
            "gid": "p1",
            "name": "Backend API",
            "permalink_url": "https://app.asana.com/0/0/p1",
        }


class TestFormatProjectOptFields:
    def test_builds_team_nested_object(self) -> None:
        row = {
            "gid": "p1",
            "name": "Backend API",
            "permalink_url": "https://app.asana.com/0/0/p1",
            "team_gid": "team1",
            "team_name": "Engineering",
            "archived": 0,
        }
        result = format_project(row, opt_fields={"team"})
        assert result["team"] == {"gid": "team1", "name": "Engineering"}

    def test_coerces_archived_to_bool(self) -> None:
        row = {
            "gid": "p1",
            "name": "Backend API",
            "permalink_url": "https://app.asana.com/0/0/p1",
            "archived": 0,
        }
        result = format_project(row, opt_fields={"archived"})
        assert result["archived"] is False

        row["archived"] = 1
        result = format_project(row, opt_fields={"archived"})
        assert result["archived"] is True


class TestFormatUserCompact:
    def test_compact_returns_only_gid_name_permalink(self) -> None:
        row = {
            "gid": "u1",
            "name": "Alice",
            "email": "alice@example.com",
        }
        result = format_user(row, opt_fields=None)
        assert result == {
            "gid": "u1",
            "name": "Alice",
            "permalink_url": None,
        }


class TestFormatTaskOptFields:
    def test_includes_requested_scalar_fields(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "notes": "Login fails on mobile",
            "due_on": "2024-02-01",
            "assignee_gid": "u1",
            "assignee_name": "Alice",
            "completed": 0,
            "custom_fields": [],
            "followers": [],
        }
        result = format_task(row, opt_fields={"notes", "due_on"})
        assert result == {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "notes": "Login fails on mobile",
            "due_on": "2024-02-01",
        }

    def test_coerces_completed_to_bool(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "completed": 1,
        }
        result = format_task(row, opt_fields={"completed"})
        assert result["completed"] is True

        row["completed"] = 0
        result = format_task(row, opt_fields={"completed"})
        assert result["completed"] is False

    def test_builds_assignee_nested_object(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "assignee_gid": "u1",
            "assignee_name": "Alice",
        }
        result = format_task(row, opt_fields={"assignee"})
        assert result["assignee"] == {"gid": "u1", "name": "Alice"}

    def test_includes_num_subtasks(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "num_subtasks": 3,
        }
        result = format_task(row, opt_fields={"num_subtasks"})
        assert result["num_subtasks"] == 3

    def test_builds_parent_nested_object_with_name(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "parent_gid": "t0",
            "parent_name": "Parent task",
        }
        result = format_task(row, opt_fields={"parent.name"})
        assert result["parent"] == {"gid": "t0", "name": "Parent task"}

    def test_null_assignee(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "assignee_gid": None,
            "assignee_name": "",
        }
        result = format_task(row, opt_fields={"assignee"})
        assert result["assignee"] is None


class TestParseOptFields:
    def test_returns_none_when_none(self) -> None:
        assert parse_opt_fields(None) is None

    def test_parses_comma_separated_fields(self) -> None:
        result = parse_opt_fields("name,completed,assignee.name,due_on")
        assert result == {"name", "completed", "assignee.name", "due_on"}

    def test_strips_whitespace(self) -> None:
        result = parse_opt_fields(" name , due_on ")
        assert result == {"name", "due_on"}


class TestFormatList:
    def test_wraps_with_next_page_when_more(self) -> None:
        query_result = {
            "results": [
                {
                    "gid": "t1",
                    "name": "Task 1",
                    "permalink_url": "https://example.com/t1",
                    "notes": "x",
                    "completed": 0,
                },
                {
                    "gid": "t2",
                    "name": "Task 2",
                    "permalink_url": "https://example.com/t2",
                    "notes": "y",
                    "completed": 0,
                },
            ],
            "count": 2,
            "total": 5,
            "has_more": True,
            "next_offset": 2,
        }
        result = format_list(query_result, "task", opt_fields=None)
        assert result == {
            "data": [
                {"gid": "t1", "name": "Task 1", "permalink_url": "https://example.com/t1"},
                {"gid": "t2", "name": "Task 2", "permalink_url": "https://example.com/t2"},
            ],
            "next_page": {"offset": "2"},
        }

    def test_next_page_null_on_last_page(self) -> None:
        query_result = {
            "results": [
                {
                    "gid": "t1",
                    "name": "Task 1",
                    "permalink_url": "https://example.com/t1",
                    "completed": 0,
                },
            ],
            "count": 1,
            "total": 1,
            "has_more": False,
        }
        result = format_list(query_result, "task", opt_fields=None)
        assert result["next_page"] is None


class TestFormatSingle:
    def test_wraps_in_data_envelope(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "notes": "x",
        }
        result = format_single(row, "task", opt_fields=None)
        assert result == {
            "data": {
                "gid": "t1",
                "name": "Fix login bug",
                "permalink_url": "https://app.asana.com/0/0/t1",
            }
        }

    def test_passes_opt_fields_through(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "notes": "Login fails",
            "completed": 1,
        }
        result = format_single(row, "task", opt_fields="notes,completed")
        assert result["data"]["notes"] == "Login fails"
        assert result["data"]["completed"] is True


class TestFormatSearchObjects:
    def test_wraps_results_in_data(self) -> None:
        query_result = {
            "results": [
                {
                    "gid": "t1",
                    "name": "Task 1",
                    "permalink_url": "https://example.com/t1",
                    "completed": 0,
                },
                {
                    "gid": "t2",
                    "name": "Task 2",
                    "permalink_url": "https://example.com/t2",
                    "completed": 0,
                },
            ],
            "count": 2,
        }
        result = format_search_objects(query_result, "task", opt_fields=None)
        assert result == {
            "data": [
                {"gid": "t1", "name": "Task 1", "permalink_url": "https://example.com/t1"},
                {"gid": "t2", "name": "Task 2", "permalink_url": "https://example.com/t2"},
            ]
        }


class TestFormatTaskTags:
    def test_tags_formatted_as_name_objects(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "tags": ["urgent", "bug"],
        }
        result = format_task(row, opt_fields={"tags"})
        assert result["tags"] == [{"name": "urgent"}, {"name": "bug"}]

    def test_projects_from_row(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "project_gid": "p1",
            "project_name": "Backend API",
        }
        result = format_task(row, opt_fields={"projects"})
        assert result["projects"] == [{"gid": "p1", "name": "Backend API"}]

    def test_empty_tags(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "tags": [],
        }
        result = format_task(row, opt_fields={"tags"})
        assert result["tags"] == []


class TestFormatTaskStories:
    def test_stories_passed_through_when_present(self) -> None:
        stories = [
            {
                "gid": "s1",
                "text": "comment text",
                "type": "comment",
                "created_by": {"name": "Alice"},
            },
        ]
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "stories": stories,
        }
        result = format_task(row, opt_fields={"notes"})
        assert result["stories"] == stories

    def test_snippet_passed_through_when_present(self) -> None:
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "snippet": "Found **login** issue in...",
        }
        result = format_task(row, opt_fields={"notes"})
        assert result["snippet"] == "Found **login** issue in..."

    def test_stories_passed_through_in_compact_mode(self) -> None:
        stories = [{"gid": "s1", "text": "hello"}]
        row = {
            "gid": "t1",
            "name": "Fix login bug",
            "permalink_url": "https://app.asana.com/0/0/t1",
            "stories": stories,
        }
        result = format_task(row, opt_fields=None)
        assert result["stories"] == stories

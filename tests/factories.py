"""Test data factory functions for building Asana export structures."""

import json
from pathlib import Path
from typing import Any


def write_json(path: str | Path, data: Any) -> None:
    """Write data as JSON to a file, creating parent dirs."""
    p = Path(path)
    p.parent.mkdir(exist_ok=True, parents=True)
    p.write_text(json.dumps(data))


def make_task(
    gid: str,
    name: str,
    project_gid: str = "proj1",
    project_name: str = "Project One",
    section_gid: str = "sec1",
    section_name: str = "To Do",
    assignee: dict[str, str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a realistic Asana task dict."""
    task: dict[str, Any] = {
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


def make_story(gid: str, text: str, resource_subtype: str = "comment_added") -> dict[str, Any]:
    """Build a realistic Asana story dict."""
    return {
        "gid": gid,
        "text": text,
        "html_text": f"<p>{text}</p>",
        "created_at": "2024-01-15T11:00:00.000Z",
        "created_by": {"gid": "user2", "name": "Bob"},
        "resource_subtype": resource_subtype,
        "type": resource_subtype,
    }


def make_attachment(gid: str, name: str) -> dict[str, Any]:
    """Build a realistic Asana attachment dict."""
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


def build_export_tree(
    root: str,
    teams: list[dict[str, Any]] | None = None,
    projects: dict[str, list[dict[str, Any]]] | None = None,
    tasks: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    stories: dict[str, list[dict[str, Any]]] | None = None,
    subtasks: dict[str, list[tuple[dict[str, Any], list[dict[str, Any]]]]] | None = None,
    attachments: dict[str, list[dict[str, Any]]] | None = None,
) -> None:
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
    write_json(root_path / "teams.json", teams)

    for team in teams:
        tgid = team["gid"]
        team_dir = root_path / "teams" / tgid
        write_json(team_dir / "projects.json", projects.get(tgid, []))

        for proj in projects.get(tgid, []):
            pgid = proj["gid"]
            proj_dir = team_dir / "projects" / pgid

            # tasks.json (summary list)
            task_list = tasks.get((tgid, pgid), [])
            write_json(
                proj_dir / "tasks.json",
                [{"gid": t["gid"], "name": t["name"]} for t in task_list],
            )

            # Individual task directories
            for task in task_list:
                task_dir = proj_dir / "tasks" / task["gid"]
                write_json(task_dir / "task.json", task)

                # Stories
                if task["gid"] in stories:
                    write_json(task_dir / "stories.json", stories[task["gid"]])

                # Attachments (individual files in attachments/ dir)
                if task["gid"] in attachments:
                    att_dir = task_dir / "attachments"
                    for att in attachments[task["gid"]]:
                        write_json(att_dir / att["gid"], att)

                # Subtasks
                if task["gid"] in subtasks:
                    for sub, sub_stories in subtasks[task["gid"]]:
                        sub_dir = task_dir / "subtasks" / sub["gid"]
                        write_json(sub_dir / "subtask.json", sub)
                        if sub_stories:
                            write_json(sub_dir / "stories.json", sub_stories)

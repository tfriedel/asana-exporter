"""MCP server exposing Asana archive search and query tools."""

import os
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations

from asana_exporter.database.queries import (
    get_project,
    get_projects,
    get_task,
    get_tasks,
    get_user,
    search_objects,
    search_tasks,
)
from asana_exporter.database.schema import migrate_schema

_DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "asana" / "asana.db"
_READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)


@dataclass
class ServerContext:
    """Shared resources for the MCP server lifetime."""

    conn: sqlite3.Connection


@asynccontextmanager
async def server_lifespan(_server: FastMCP) -> AsyncIterator[ServerContext]:
    """Open database on startup, close on shutdown."""
    db_path_str = os.environ.get("ASANA_DB_PATH")
    db_path = Path(db_path_str) if db_path_str else _DEFAULT_DB_PATH

    if not db_path.exists():
        msg = (
            f"Database not found at {db_path}. "
            "Run 'asana-exporter' first to export and import data, "
            "or set ASANA_DB_PATH environment variable."
        )
        raise FileNotFoundError(msg)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    migrate_schema(conn)

    try:
        yield ServerContext(conn=conn)
    finally:
        conn.close()


mcp_server = FastMCP(
    "asana-archive",
    instructions="""\
Search and browse an offline Asana archive (projects, tasks, users).

## Recommended Workflow
1. Use asana_search_tasks for full-text search across all tasks.
2. Use asana_get_projects / asana_get_tasks for filtered listing.
3. Use asana_get_task for full details on a specific task.
4. Use asana_search_objects for quick typeahead across object types.

## Tips
- All data is from an offline SQLite archive. Data may not reflect
  real-time changes in Asana.
- Use use_holistic=true in search_tasks to also search subtask and
  comment text (not just the task itself).
- Pagination: when has_more is true, use next_offset in a follow-up call.
""",
    lifespan=server_lifespan,
)


def _ctx(mcp_ctx: Context) -> ServerContext:
    return mcp_ctx.request_context.lifespan_context  # type: ignore[return-value]


# ── MCP Tool Wrappers ────────────────────────────────────────────────────


@mcp_server.tool(name="asana_search_tasks", annotations=_READ_ONLY)
async def asana_search_tasks_tool(
    ctx: Context,
    query: str,
    project_gid: str | None = None,
    assignee: str | None = None,
    completed: bool | None = None,
    use_holistic: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Full-text search across tasks. Searches name, notes, assignee, project, and tags.

    Set use_holistic=true to also search subtask text and comment text.
    """
    return search_tasks(
        _ctx(ctx).conn,
        query=query,
        project_gid=project_gid,
        assignee=assignee,
        completed=completed,
        use_holistic=use_holistic,
        limit=limit,
        offset=offset,
    )


@mcp_server.tool(name="asana_get_task", annotations=_READ_ONLY)
async def asana_get_task_tool(
    ctx: Context,
    task_gid: str,
    include_subtasks: bool = False,
    include_stories: bool = False,
) -> dict[str, Any]:
    """Get full task details by GID. Optionally include subtasks and comments."""
    return get_task(
        _ctx(ctx).conn,
        task_gid=task_gid,
        include_subtasks=include_subtasks,
        include_stories=include_stories,
    )


@mcp_server.tool(name="asana_get_tasks", annotations=_READ_ONLY)
async def asana_get_tasks_tool(
    ctx: Context,
    project_gid: str | None = None,
    section_gid: str | None = None,
    assignee_gid: str | None = None,
    completed: bool | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """List tasks with filters. Provide at least one filter (project, section, assignee)."""
    return get_tasks(
        _ctx(ctx).conn,
        project_gid=project_gid,
        section_gid=section_gid,
        assignee_gid=assignee_gid,
        completed=completed,
        limit=limit,
        offset=offset,
    )


@mcp_server.tool(name="asana_get_project", annotations=_READ_ONLY)
async def asana_get_project_tool(
    ctx: Context,
    project_gid: str,
    include_sections: bool = True,
) -> dict[str, Any]:
    """Get project details by GID, including sections."""
    return get_project(
        _ctx(ctx).conn,
        project_gid=project_gid,
        include_sections=include_sections,
    )


@mcp_server.tool(name="asana_get_projects", annotations=_READ_ONLY)
async def asana_get_projects_tool(
    ctx: Context,
    team_gid: str | None = None,
    archived: bool | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """List projects with optional team and archived filters."""
    return get_projects(
        _ctx(ctx).conn,
        team_gid=team_gid,
        archived=archived,
        limit=limit,
        offset=offset,
    )


@mcp_server.tool(name="asana_get_user", annotations=_READ_ONLY)
async def asana_get_user_tool(
    ctx: Context,
    user_gid: str | None = None,
    email: str | None = None,
) -> dict[str, Any]:
    """Look up a user by GID or email."""
    return get_user(
        _ctx(ctx).conn,
        user_gid=user_gid,
        email=email,
    )


@mcp_server.tool(name="asana_search_objects", annotations=_READ_ONLY)
async def asana_search_objects_tool(
    ctx: Context,
    query: str,
    resource_type: str,
    count: int = 20,
) -> dict[str, Any]:
    """Quick typeahead search. resource_type: project, task, user, or team."""
    return search_objects(
        _ctx(ctx).conn,
        query=query,
        resource_type=resource_type,
        count=count,
    )


def run_mcp_server() -> None:
    """Run the MCP server with stdio transport."""
    mcp_server.run(transport="stdio")

"""MCP server exposing Asana archive search and query tools."""

import os
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

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
from asana_exporter.mcp.formatters import (
    format_list,
    format_search_objects,
    format_single,
)

# Common parameter descriptions, reused across tools.
_D_OPT_FIELDS = (
    "Comma-separated list of optional fields to include."
)
_D_LIMIT = "Results per page (1-100)."
_D_OFFSET = "Pagination offset from a previous next_page."
_D_PROJECT = "Globally unique identifier for the project."
_D_SECTION = "Globally unique identifier for the section."
_D_COMPLETED = "Filter to completed or incomplete tasks only."
_D_TASK_ID = "Globally unique identifier for the task."
_D_USER_ID = (
    "Identifier for a user. Can be an email or a GID."
)
_D_QUERY = (
    "Search query. Can be empty for default results."
)
_D_RESOURCE_TYPE = (
    "Resource type to search: project, task, user, or team."
)
_D_COUNT = "Number of results to return (1-100)."
_D_ASSIGNEE = "User GID to filter tasks by assignee."
_D_HOLISTIC = (
    "Also search subtask and comment text."
)
_D_SUBTASKS = "Include the task's subtasks in the response."
_D_STORIES = (
    "Include comments and activity stories for the task."
)

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
Mirrors the official Asana MCP tool interface for compatibility.

## Recommended Workflow
1. Use search_objects for quick typeahead across object types.
2. Use search_tasks for full-text search across all tasks.
3. Use get_projects / get_tasks for filtered listing.
4. Use get_task for full details on a specific task.

## Tips
- All data is from an offline SQLite archive. Data may not reflect
  real-time changes in Asana.
- Use opt_fields to request specific fields (e.g. "name,completed,assignee.name").
  Without opt_fields, only compact data (gid, name, permalink_url) is returned.
- Use use_holistic=true in search_tasks to also search subtask and
  comment text (not just the task itself).
- Pagination: when next_page is not null, use its offset in a follow-up call.
""",
    lifespan=server_lifespan,
)


def _ctx(mcp_ctx: Context) -> ServerContext:
    return mcp_ctx.request_context.lifespan_context  # type: ignore[return-value]


# ── MCP Tool Wrappers ────────────────────────────────────────────────────


@mcp_server.tool(
    name="search_tasks",
    description=(
        "Full-text search across tasks. Searches name, notes,"
        " assignee, project, and tags."
        " Set use_holistic=true to also search subtask and"
        " comment text."
    ),
    annotations=_READ_ONLY,
)
async def search_tasks_tool(
    ctx: Context,
    query: Annotated[str, Field(description=_D_QUERY)],
    project: Annotated[str | None, Field(description=_D_PROJECT)] = None,
    assignee: Annotated[str | None, Field(description=_D_ASSIGNEE)] = None,
    completed: Annotated[bool | None, Field(description=_D_COMPLETED)] = None,
    use_holistic: Annotated[bool, Field(description=_D_HOLISTIC)] = False,
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
    limit: Annotated[int, Field(description=_D_LIMIT, ge=1, le=100)] = 20,
    offset: Annotated[int, Field(description=_D_OFFSET)] = 0,
) -> dict[str, Any]:
    result = search_tasks(
        _ctx(ctx).conn,
        query=query,
        project_gid=project,
        assignee=assignee,
        completed=completed,
        use_holistic=use_holistic,
        limit=limit,
        offset=offset,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_list(result, "task", opt_fields)


@mcp_server.tool(
    name="get_task",
    description=(
        "Get full task details by ID. Returns name, notes,"
        " assignee, due dates, projects, tags."
        " Use opt_fields for specific fields."
        " Set include_subtasks or include_stories for"
        " related data."
    ),
    annotations=_READ_ONLY,
)
async def get_task_tool(
    ctx: Context,
    task_id: Annotated[str, Field(description=_D_TASK_ID)],
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
    include_subtasks: Annotated[bool, Field(description=_D_SUBTASKS)] = False,
    include_stories: Annotated[bool, Field(description=_D_STORIES)] = False,
) -> dict[str, Any]:
    result = get_task(
        _ctx(ctx).conn,
        task_gid=task_id,
        include_subtasks=include_subtasks,
        include_stories=include_stories,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_single(result, "task", opt_fields)


@mcp_server.tool(
    name="get_tasks",
    description=(
        "List tasks filtered by project, section, or assignee."
        " At least one filter required."
        " Returns task names and IDs."
        " Use for browsing project contents."
    ),
    annotations=_READ_ONLY,
)
async def get_tasks_tool(
    ctx: Context,
    project: Annotated[str | None, Field(description=_D_PROJECT)] = None,
    section: Annotated[str | None, Field(description=_D_SECTION)] = None,
    assignee: Annotated[str | None, Field(description=_D_ASSIGNEE)] = None,
    completed: Annotated[bool | None, Field(description=_D_COMPLETED)] = None,
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
    limit: Annotated[int, Field(description=_D_LIMIT, ge=1, le=100)] = 20,
    offset: Annotated[int, Field(description=_D_OFFSET)] = 0,
) -> dict[str, Any]:
    result = get_tasks(
        _ctx(ctx).conn,
        project_gid=project,
        section_gid=section,
        assignee_gid=assignee,
        completed=completed,
        limit=limit,
        offset=offset,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_list(result, "task", opt_fields)


@mcp_server.tool(
    name="get_project",
    description=(
        "Get detailed project data by ID including name,"
        " description, owner, team, and sections."
        " Use after finding project ID via search_objects."
        " Specify opt_fields for additional details."
    ),
    annotations=_READ_ONLY,
)
async def get_project_tool(
    ctx: Context,
    project_id: Annotated[str, Field(description=_D_PROJECT)],
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
) -> dict[str, Any]:
    include_sections = opt_fields is not None and "sections" in opt_fields
    result = get_project(
        _ctx(ctx).conn,
        project_gid=project_id,
        include_sections=include_sections,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_single(result, "project", opt_fields)


@mcp_server.tool(
    name="get_projects",
    description=(
        "List projects with optional team and archived filters."
        " Returns project names and IDs."
        " Use for finding project GIDs."
    ),
    annotations=_READ_ONLY,
)
async def get_projects_tool(
    ctx: Context,
    team: Annotated[str | None, Field(description="Filter by team GID.")] = None,
    archived: Annotated[bool | None, Field(description="Include archived projects.")] = None,
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
    limit: Annotated[int, Field(description=_D_LIMIT, ge=1, le=100)] = 20,
    offset: Annotated[int, Field(description=_D_OFFSET)] = 0,
) -> dict[str, Any]:
    result = get_projects(
        _ctx(ctx).conn,
        team_gid=team,
        archived=archived,
        limit=limit,
        offset=offset,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_list(result, "project", opt_fields)


@mcp_server.tool(
    name="get_user",
    description=(
        "Get user details by GID or email."
        " Returns name and email."
        " Use to find user IDs for filtering tasks by assignee."
    ),
    annotations=_READ_ONLY,
)
async def get_user_tool(
    ctx: Context,
    user_id: Annotated[str | None, Field(description=_D_USER_ID)] = None,
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
) -> dict[str, Any]:
    conn = _ctx(ctx).conn
    if user_id and "@" in user_id:
        result = get_user(conn, email=user_id)
    else:
        result = get_user(conn, user_gid=user_id)
    if "error" in result:
        return {"error": result["error"]}
    return format_single(result, "user", opt_fields)


@mcp_server.tool(
    name="search_objects",
    description=(
        "Quick search across Asana objects by name."
        " Use this first before specialized search tools."
        " Faster than search_tasks for finding specific"
        " items by name."
    ),
    annotations=_READ_ONLY,
)
async def search_objects_tool(
    ctx: Context,
    query: Annotated[str, Field(description=_D_QUERY)],
    resource_type: Annotated[str, Field(description=_D_RESOURCE_TYPE)],
    count: Annotated[int, Field(description=_D_COUNT, ge=1, le=100)] = 20,
    opt_fields: Annotated[str | None, Field(description=_D_OPT_FIELDS)] = None,
) -> dict[str, Any]:
    result = search_objects(
        _ctx(ctx).conn,
        query=query,
        resource_type=resource_type,
        count=count,
    )
    if "error" in result:
        return {"error": result["error"]}
    return format_search_objects(result, resource_type, opt_fields)


def run_mcp_server() -> None:
    """Run the MCP server with stdio transport."""
    mcp_server.run(transport="stdio")

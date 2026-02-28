"""Query functions for the Asana archive SQLite database."""

import json
import sqlite3
from typing import Any

_JSON_COLUMNS = ("tags", "custom_fields", "dependencies", "dependents", "followers")


def get_user(
    conn: sqlite3.Connection,
    *,
    user_gid: str | None = None,
    email: str | None = None,
) -> dict[str, Any]:
    """Look up a user by GID or email."""
    if user_gid:
        row = conn.execute("SELECT * FROM users WHERE gid = ?", (user_gid,)).fetchone()
    elif email:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    else:
        return {"error": "Provide either user_gid or email."}

    if not row:
        return {"error": "User not found."}

    return dict(row)


def get_project(
    conn: sqlite3.Connection,
    *,
    project_gid: str,
    include_sections: bool = True,
) -> dict[str, Any]:
    """Get project details by GID, optionally with sections."""
    row = conn.execute("SELECT * FROM projects WHERE gid = ?", (project_gid,)).fetchone()
    if not row:
        return {"error": f"Project '{project_gid}' not found."}

    result = dict(row)

    if include_sections:
        section_rows = conn.execute(
            "SELECT * FROM sections WHERE project_gid = ? ORDER BY rowid",
            (project_gid,),
        ).fetchall()
        result["sections"] = [dict(s) for s in section_rows]

    return result


def get_tasks(
    conn: sqlite3.Connection,
    *,
    project_gid: str | None = None,
    section_gid: str | None = None,
    assignee_gid: str | None = None,
    completed: bool | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """List tasks with optional filters and pagination."""
    conditions: list[str] = []
    params: list[Any] = []

    if project_gid is not None:
        conditions.append("project_gid = ?")
        params.append(project_gid)
    if section_gid is not None:
        conditions.append("section_gid = ?")
        params.append(section_gid)
    if assignee_gid is not None:
        conditions.append("assignee_gid = ?")
        params.append(assignee_gid)
    if completed is not None:
        conditions.append("completed = ?")
        params.append(int(completed))

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    total = conn.execute(f"SELECT COUNT(*) FROM tasks{where}", params).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM tasks{where} ORDER BY created_at LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()

    return _paginated_result([_task_row_to_dict(r) for r in rows], total, limit, offset)


def _paginated_result(
    results: list[dict[str, Any]],
    total: int,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    """Build a standardized pagination envelope."""
    output: dict[str, Any] = {
        "results": results,
        "count": len(results),
        "total": total,
        "has_more": offset + len(results) < total,
    }
    if output["has_more"]:
        output["next_offset"] = offset + limit
    return output


def get_projects(
    conn: sqlite3.Connection,
    *,
    team_gid: str | None = None,
    archived: bool | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """List projects with optional filters and pagination."""
    conditions: list[str] = []
    params: list[Any] = []

    if team_gid is not None:
        conditions.append("team_gid = ?")
        params.append(team_gid)
    if archived is not None:
        conditions.append("archived = ?")
        params.append(int(archived))

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    total = conn.execute(f"SELECT COUNT(*) FROM projects{where}", params).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM projects{where} ORDER BY name LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()

    return _paginated_result([dict(r) for r in rows], total, limit, offset)


def _task_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a task row to a dict, parsing JSON columns and stripping raw_json."""
    d = dict(row)
    for col in _JSON_COLUMNS:
        if col in d and isinstance(d[col], str):
            d[col] = json.loads(d[col])
    d.pop("raw_json", None)
    return d


def get_task(
    conn: sqlite3.Connection,
    *,
    task_gid: str,
    include_subtasks: bool = False,
    include_stories: bool = False,
) -> dict[str, Any]:
    """Get full task details by GID."""
    row = conn.execute("SELECT * FROM tasks WHERE gid = ?", (task_gid,)).fetchone()
    if not row:
        return {"error": f"Task '{task_gid}' not found."}

    result = _task_row_to_dict(row)

    if include_subtasks:
        subtask_rows = conn.execute(
            "SELECT * FROM tasks WHERE parent_gid = ? ORDER BY created_at",
            (task_gid,),
        ).fetchall()
        result["subtasks"] = [_task_row_to_dict(s) for s in subtask_rows]

    if include_stories:
        story_rows = conn.execute(
            "SELECT * FROM stories WHERE task_gid = ? ORDER BY created_at",
            (task_gid,),
        ).fetchall()
        result["stories"] = [dict(s) for s in story_rows]

    return result


def _prepare_fts_query(query: str) -> str:
    """Convert user query to FTS5 query with prefix matching.

    Words 3+ chars get * suffix for prefix matching. AND/OR/NOT preserved.
    Quoted phrases preserved as-is.
    """
    import re

    tokens: list[str] = []
    parts = re.split(r'(".*?")', query)
    for part in parts:
        if part.startswith('"') and part.endswith('"'):
            tokens.append(part)
        else:
            for word in part.split():
                upper = word.upper()
                if upper in ("AND", "OR", "NOT"):
                    tokens.append(upper)
                else:
                    cleaned = re.sub(r"[^\w]", "", word, flags=re.UNICODE)
                    if cleaned:
                        if len(cleaned) >= 3:
                            tokens.append(f"{cleaned}*")
                        else:
                            tokens.append(cleaned)
    return " ".join(tokens)


def search_tasks(
    conn: sqlite3.Connection,
    *,
    query: str,
    project_gid: str | None = None,
    assignee: str | None = None,
    completed: bool | None = None,
    use_holistic: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Search tasks using FTS5.

    Uses tasks_fts for per-entity search, or task_search_fts for holistic
    search (combines task + subtasks + comments into one document).
    """
    if not query.strip():
        return {"error": "Query must not be empty."}

    fts_query = _prepare_fts_query(query)
    if not fts_query:
        return {"error": "Query must contain searchable terms."}

    if use_holistic:
        return _search_holistic(conn, fts_query, project_gid, completed, limit, offset)
    return _search_per_entity(conn, fts_query, project_gid, assignee, completed, limit, offset)


def _search_per_entity(
    conn: sqlite3.Connection,
    fts_query: str,
    project_gid: str | None,
    assignee: str | None,
    completed: bool | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    """Search tasks_fts (per-entity: each task/subtask is a separate document)."""
    conditions = ["tasks_fts MATCH ?"]
    params: list[Any] = [fts_query]

    if project_gid is not None:
        conditions.append("t.project_gid = ?")
        params.append(project_gid)
    if assignee is not None:
        conditions.append("(t.assignee_gid = ? OR t.assignee_name LIKE ?)")
        params.extend([assignee, f"%{assignee}%"])
    if completed is not None:
        conditions.append("t.completed = ?")
        params.append(int(completed))

    where = " AND ".join(conditions)

    count_sql = f"""
        SELECT COUNT(*) FROM tasks_fts
        JOIN tasks t ON t.rowid = tasks_fts.rowid
        WHERE {where}
    """
    total = conn.execute(count_sql, params).fetchone()[0]

    select_sql = f"""
        SELECT t.*, snippet(tasks_fts, 0, '**', '**', '...', 32) as snippet
        FROM tasks_fts
        JOIN tasks t ON t.rowid = tasks_fts.rowid
        WHERE {where}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(select_sql, [*params, limit, offset]).fetchall()

    results = []
    for row in rows:
        d = _task_row_to_dict(row)
        d["snippet"] = row["snippet"]
        results.append(d)

    return _paginated_result(results, total, limit, offset)


def _search_holistic(
    conn: sqlite3.Connection,
    fts_query: str,
    project_gid: str | None,
    completed: bool | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    """Search task_search_fts (holistic: task + subtasks + comments combined)."""
    conditions = ["task_search_fts MATCH ?"]
    params: list[Any] = [fts_query]

    join_conditions: list[str] = []
    if project_gid is not None:
        join_conditions.append("t.project_gid = ?")
        params.append(project_gid)
    if completed is not None:
        join_conditions.append("t.completed = ?")
        params.append(int(completed))

    where = " AND ".join(conditions)
    join_where = f" AND {' AND '.join(join_conditions)}" if join_conditions else ""

    count_sql = f"""
        SELECT COUNT(*) FROM task_search_fts
        JOIN tasks t ON t.gid = task_search_fts.task_gid
        WHERE {where}{join_where}
    """
    total = conn.execute(count_sql, params).fetchone()[0]

    select_sql = f"""
        SELECT t.*, snippet(task_search_fts, 0, '**', '**', '...', 32) as snippet
        FROM task_search_fts
        JOIN tasks t ON t.gid = task_search_fts.task_gid
        WHERE {where}{join_where}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(select_sql, [*params, limit, offset]).fetchall()

    results = []
    for row in rows:
        d = _task_row_to_dict(row)
        d["snippet"] = row["snippet"]
        results.append(d)

    return _paginated_result(results, total, limit, offset)


_SEARCH_OBJECTS_TABLES: dict[str, tuple[str, str]] = {
    "project": ("projects", "name"),
    "task": ("tasks", "name"),
    "user": ("users", "name"),
    "team": ("teams", "name"),
}


def search_objects(
    conn: sqlite3.Connection,
    *,
    query: str,
    resource_type: str,
    count: int = 20,
) -> dict[str, Any]:
    """Quick LIKE-based typeahead search across object types."""
    if resource_type not in _SEARCH_OBJECTS_TABLES:
        valid = ", ".join(sorted(_SEARCH_OBJECTS_TABLES))
        return {"error": f"Invalid resource_type '{resource_type}'. Must be one of: {valid}"}

    table, name_col = _SEARCH_OBJECTS_TABLES[resource_type]

    if query.strip():
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE {name_col} LIKE ? ORDER BY {name_col} LIMIT ?",
            (f"%{query}%", count),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT * FROM {table} ORDER BY {name_col} LIMIT ?",
            (count,),
        ).fetchall()

    results = [dict(r) for r in rows]
    return {"results": results, "count": len(results)}

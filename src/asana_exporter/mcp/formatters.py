"""Response formatters to match the official Asana MCP response format."""

from typing import Any

_COMPACT_FIELDS = ("gid", "name", "permalink_url")

# Scalar fields that can be copied directly from a task row.
_TASK_SCALAR_FIELDS = frozenset(
    {
        "notes",
        "html_notes",
        "due_on",
        "due_at",
        "start_on",
        "start_at",
        "created_at",
        "modified_at",
        "completed_at",
        "resource_subtype",
        "num_likes",
        "num_subtasks",
    }
)

# Scalar fields that can be copied directly from a project row.
_PROJECT_SCALAR_FIELDS = frozenset(
    {
        "description",
        "created_at",
        "modified_at",
        "due_date",
    }
)

# Fields stored as int 0/1 in DB but should be bool in responses.
_BOOL_FIELDS = frozenset({"completed", "archived"})

# Nested object mappings: opt_field name -> (gid_column, name_column).
# When gid_column is null/empty, the nested object is null.
_TASK_NESTED_FIELDS: dict[str, tuple[str, str]] = {
    "assignee": ("assignee_gid", "assignee_name"),
    "parent": ("parent_gid", "parent_name"),
    "team": ("team_gid", "team_name"),
    "section": ("section_gid", "section_name"),
}

_PROJECT_NESTED_FIELDS: dict[str, tuple[str, str]] = {
    "team": ("team_gid", "team_name"),
    "owner": ("owner_gid", ""),  # name requires lookup
}


_FORMAT_FNS: dict[str, Any] = {}  # populated after function definitions


def parse_opt_fields(opt_fields: str | None) -> set[str] | None:
    """Parse a comma-separated opt_fields string into a set of field names."""
    if opt_fields is None:
        return None
    return {f.strip() for f in opt_fields.split(",") if f.strip()}


def _compact(row: dict[str, Any]) -> dict[str, Any]:
    """Return compact representation: gid, name, permalink_url."""
    return {k: row.get(k) for k in _COMPACT_FIELDS}


def _add_scalar_fields(
    result: dict[str, Any],
    row: dict[str, Any],
    opt_fields: set[str],
    allowed: frozenset[str],
) -> None:
    """Add requested scalar fields from row to result."""
    for field in opt_fields & allowed:
        result[field] = row.get(field)
    for field in opt_fields & _BOOL_FIELDS:
        if field in row:
            result[field] = bool(row[field])


def _build_nested(
    row: dict[str, Any],
    gid_col: str,
    name_col: str,
) -> dict[str, str] | None:
    """Build a nested {gid, name} object from flat DB columns."""
    gid = row.get(gid_col)
    if not gid:
        return None
    obj: dict[str, str] = {"gid": gid}
    if name_col:
        name = row.get(name_col, "")
        if name:
            obj["name"] = name
    return obj


def _add_nested_fields(
    result: dict[str, Any],
    row: dict[str, Any],
    opt_fields: set[str],
    nested_map: dict[str, tuple[str, str]],
) -> None:
    """Add requested nested object fields to result."""
    for field, (gid_col, name_col) in nested_map.items():
        if field in opt_fields or f"{field}.name" in opt_fields:
            result[field] = _build_nested(row, gid_col, name_col)


_PASSTHROUGH_KEYS = ("subtasks", "stories", "snippet")


def _compact_with_collections(row: dict[str, Any]) -> dict[str, Any]:
    """Return compact representation, preserving sub-collections like stories/subtasks."""
    result = _compact(row)
    for key in _PASSTHROUGH_KEYS:
        if key in row:
            result[key] = row[key]
    return result


def format_task(
    row: dict[str, Any],
    *,
    opt_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Format a task row for MCP response."""
    if opt_fields is None:
        return _compact_with_collections(row)
    result = _compact(row)
    _add_scalar_fields(result, row, opt_fields, _TASK_SCALAR_FIELDS)
    _add_nested_fields(result, row, opt_fields, _TASK_NESTED_FIELDS)
    # Projects: build from row's project_gid/project_name
    if "projects" in opt_fields or "projects.name" in opt_fields:
        pgid = row.get("project_gid")
        if pgid:
            result["projects"] = [{"gid": pgid, "name": row.get("project_name", "")}]
        else:
            result["projects"] = []
    # Tags: convert string list to name objects
    if "tags" in opt_fields or "tags.name" in opt_fields:
        tags = row.get("tags", [])
        result["tags"] = [{"name": t} for t in tags] if tags else []
    # Pass through search snippets and sub-collections (subtasks/stories)
    for key in _PASSTHROUGH_KEYS:
        if key in row:
            result[key] = row[key]
    return result


def format_project(
    row: dict[str, Any],
    *,
    opt_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Format a project row for MCP response."""
    if opt_fields is None:
        return _compact(row)
    result = _compact(row)
    _add_scalar_fields(result, row, opt_fields, _PROJECT_SCALAR_FIELDS)
    _add_nested_fields(result, row, opt_fields, _PROJECT_NESTED_FIELDS)
    # Official API uses "notes" but our DB stores "description"
    if "notes" in opt_fields:
        result["notes"] = row.get("description", "")
    # Sections: include when requested
    if ("sections" in opt_fields or "sections.name" in opt_fields) and "sections" in row:
        sections = row["sections"]
        result["sections"] = [{"gid": s["gid"], "name": s["name"]} for s in sections]
    return result


def format_user(
    row: dict[str, Any],
    *,
    opt_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Format a user row for MCP response."""
    if opt_fields is None:
        return _compact(row)
    result = _compact(row)
    if "email" in opt_fields:
        result["email"] = row.get("email")
    return result


def _format_generic(
    row: dict[str, Any],
    *,
    opt_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Fallback formatter for resource types without specific formatting."""
    return _compact(row)


# Register format functions by resource type.
_FORMAT_FNS.update(
    {
        "task": format_task,
        "project": format_project,
        "user": format_user,
        "team": _format_generic,
    }
)


def format_single(
    data: dict[str, Any],
    resource_type: str,
    opt_fields: str | None,
) -> dict[str, Any]:
    """Wrap a single resource in the Asana response envelope."""
    fields = parse_opt_fields(opt_fields)
    fmt = _FORMAT_FNS.get(resource_type, _format_generic)
    return {"data": fmt(data, opt_fields=fields)}


def format_list(
    query_result: dict[str, Any],
    resource_type: str,
    opt_fields: str | None,
) -> dict[str, Any]:
    """Wrap a paginated query result in the Asana response envelope."""
    fields = parse_opt_fields(opt_fields)
    fmt = _FORMAT_FNS.get(resource_type, _format_generic)
    items = [fmt(row, opt_fields=fields) for row in query_result["results"]]
    next_page = None
    if query_result.get("has_more"):
        next_page = {"offset": str(query_result["next_offset"])}
    return {"data": items, "next_page": next_page}


def format_search_objects(
    query_result: dict[str, Any],
    resource_type: str,
    opt_fields: str | None,
) -> dict[str, Any]:
    """Wrap search_objects results in the Asana response envelope (no pagination)."""
    fields = parse_opt_fields(opt_fields)
    fmt = _FORMAT_FNS.get(resource_type, _format_generic)
    items = [fmt(row, opt_fields=fields) for row in query_result["results"]]
    return {"data": items}

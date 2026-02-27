# Asana Exporter - SQLite Database Progress

## What was done

Designed and implemented a SQLite schema for the asana-exporter, adding a database layer on top of the existing JSON file exports. The goal is full-text search over Asana tasks, subtasks, and comments.

### Research

- Cloned and assessed `Asana/asana2sql` — dead project (last commit 2019, explicitly deprecated, legacy SDK). Useful only as a schema reference.
- Studied two existing projects as patterns to follow:
  - **mattermost_archive**: FTS5 with `porter unicode61` tokenizer, external content mode, schema versioning via `metadata` table, manual FTS sync
  - **dynalist-archive**: FTS5 with triggers for auto-sync, hierarchical data with `parent_id`/`depth`/`path` columns, `sync_state` table for incremental imports

### Schema created

New files: `asana_exporter/database/__init__.py` and `asana_exporter/database/schema.py`

**10 tables:**

| Table              | Purpose                                                                                             |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| `metadata`         | Schema versioning (key-value)                                                                       |
| `sync_state`       | Tracks imported JSON files by path + content hash                                                   |
| `users`            | Reference table (gid, name, email)                                                                  |
| `teams`            | (gid, name)                                                                                         |
| `projects`         | Project metadata with team FK                                                                       |
| `sections`         | Sections within projects                                                                            |
| `tasks`            | Unified for tasks AND subtasks (parent_gid hierarchy, denormalized assignee_name/project_name/etc.) |
| `task_memberships` | Join table for tasks in multiple projects/sections                                                  |
| `stories`          | Comments + system events on tasks                                                                   |
| `attachments`      | File attachments on tasks                                                                           |

**2 FTS5 tables:**

1. **`tasks_fts`** — Per-entity search, auto-synced via INSERT/DELETE/UPDATE triggers. Indexes: name, notes, assignee_name, project_name, section_name, tags. Use case: find a specific task or subtask.

2. **`task_search_fts`** — Holistic search, rebuilt after import via `rebuild_task_search_fts()`. Aggregates per top-level task: task text + all descendant subtask names/notes + all comments (including on subtasks at any depth, via recursive CTE). BM25 column weights: task_name=10, task_notes=5, subtask_text=2, comment_text=1. Use case: "find the task about X" where X may only appear in a subtask or comment.

**Key design decisions:**

- Tasks + subtasks in one table with `parent_gid` (like dynalist's `nodes`)
- Custom fields stored as JSON column (not FTS-indexed for now)
- Tags stored as JSON array of name strings (`["urgent","bug"]`)
- Only comment stories indexed in FTS (system events excluded)
- Denormalized assignee_name, project_name etc. on tasks for display + FTS
- ISO-8601 text timestamps (matching Asana's native format)
- `raw_json` column as escape hatch for un-modeled fields

**Helper functions:** `create_schema`, `migrate_schema`, `get/set_metadata`, `rebuild_task_search_fts`

### Verified

All tests pass:

- Schema creates without errors in memory
- FTS triggers work correctly on INSERT, UPDATE, DELETE
- Tags are searchable via per-entity FTS
- Holistic search finds tasks by subtask name, subtask notes, subtask comments, and sub-subtask comments (depth=2)
- System events correctly excluded from holistic FTS
- BM25 weighted ranking works
- `migrate_schema` works on fresh DB and is idempotent
- Multiple top-level tasks indexed correctly with proper isolation

### JSON-to-SQLite loader implemented

New file: `asana_exporter/database/loader.py`

**Entry point:** `import_export_dir(conn, export_path, force=False) -> ImportStats`

Walks the JSON export directory tree and populates all SQLite tables:

1. Teams from `teams.json`
2. Projects from each team's `projects.json` (all inserted first for FK safety)
3. Tasks from individual `task.json` files, with recursive subtask traversal
4. Stories from `stories.json` per task/subtask
5. Attachments from individual metadata files in `attachments/` dirs
6. Users extracted inline from assignee, created_by, completed_by, etc.
7. Sections and task_memberships extracted from task membership data
8. `rebuild_task_search_fts()` called after import

**Incremental sync:** SHA-256 hash of each project's `tasks.json` stored in `sync_state`. Unchanged projects are skipped on re-import. `--force-update` bypasses this.

**CLI:** `asana-exporter --export-path asana_export --to-sqlite [--db path.db] [--force-update]`

**Tests:** `tests/test_loader.py` — 14 tests covering basic import, incremental skip/reimport/force, user extraction, FTS triggers, holistic FTS rebuild, task memberships (single/multi-project), and sections.

## Next steps

- Add a search CLI or MCP server for querying the database

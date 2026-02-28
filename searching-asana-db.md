# Searching the Asana SQLite Database

## Database Location

- Export data: `/home/thomas/.local/share/asana/` (15GB JSON)
- SQLite DB: `/home/thomas/.local/share/asana/asana.db` (~940MB)
- Build with: `python -m asana_exporter.client --to-sqlite --export-path /home/thomas/.local/share/asana`

## Key Schema Facts

- Stories `resource_subtype` for comments is `'comment_added'`, NOT `'comment'`. This tripped up the search badly — zero results until corrected.
- FTS table `task_search_fts` columns: `task_name, task_notes, subtask_text, comment_text, assignee_name, project_name, task_gid` — note `task_gid` is UNINDEXED (stored but not searchable). No `gid` column directly.
- `task_search_fts` is holistic: one row per top-level task, aggregating all subtask text and comments. Good for broad searches but coarse.

## Search Strategy for Vague Queries

When the user describes something conversationally ("the ticket where we talked about X"), the information could be in:
1. Task name
2. Task notes/description
3. **Comments (stories)** — most likely place for casual mentions
4. Subtask names/notes

### What works

1. **Start with FTS** for a quick broad sweep, but know it uses porter stemming — exact field names like `user_id` may not stem well.
2. **Fall back to LIKE queries on stories** joined to tasks — this is where conversational mentions live. Critical: use `resource_subtype = 'comment_added'` for actual comments.
3. **Search comments broadly** — the answer was in a comment on a ticket with a completely different title ("documentation: some fields should be required, not optional"). The ticket title gave no hint about "falcon writes user_id".
4. **Cast a wide net on comments**: search for key nouns (`falcon`, `user_id`) without requiring verbs like "write"/"store". People paraphrase.

### What didn't work / wasted time

- Searching only task names and notes — the discussion was in comments.
- Using `resource_subtype = 'comment'` — this matched zero rows because the actual value is `'comment_added'`.
- Over-constraining with too many terms (`falcon AND user_id AND write`) — too specific for a casual mention.
- FTS alone — the holistic `task_search_fts` aggregates comment text but ranking didn't surface this well because the comment was one of many on the task.

### Recommended approach

```sql
-- Step 1: Quick FTS sweep
SELECT task_gid, task_name, substr(comment_text, 1, 300)
FROM task_search_fts
WHERE task_search_fts MATCH 'falcon user_id'
ORDER BY rank LIMIT 10;

-- Step 2: Direct comment search (the money query)
SELECT s.task_gid, t.name, t.project_name, s.created_by_name,
       substr(s.text, 1, 600)
FROM stories s
JOIN tasks t ON s.task_gid = t.gid
WHERE s.resource_subtype = 'comment_added'
  AND lower(s.text) LIKE '%falcon%'
  AND lower(s.text) LIKE '%user_id%'
LIMIT 20;

-- Step 3: Broaden if needed — drop one keyword or use shorter fragments
-- e.g. just '%falcon%' AND '%user%'
```

The direct comment search (Step 2) is what found the answer. If done first with the correct `resource_subtype`, the search would have been much faster.

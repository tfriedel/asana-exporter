# Overview

Exports Asana projects along with their resources such as tasks, stories and attachments and saves as json in a tree structure making it easy to query with tools like [jq](https://stedolan.github.io/jq/) and import into other tools like Jira.

## Install

From pypi:

```
sudo apt install python3-pip
pip install asana-exporter
```

From snap:

```
sudo snap install asana-exporter
```

From source:

```
sudo apt install python3-pip
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python setup.py build
python setup.py install
```

## Usage

The first action is to extract information from Asana using the API. To do this you will need to generate a Personal Access Token from the web UI - see https://developers.asana.com/docs/personal-access-token for instructions on how to do this.

Once you have a token, run this tool as follows to extract data. A team name and workspace are required (see https://developers.asana.com/docs/workspaces - this is usually your organisation name). Projects are extracted in the context of a team. You can extract multiple teams' projects into the same archive by running the tool multiple times with different teams.

If you want to start by getting a list of available teams you can do:

```
asana-exporter --token TOKEN --workspace WORKSPACE --export-path EXPORT_PATH --list-teams
```

Now you can choose a team and extract all of its projects:

```
asana-exporter --token TOKEN --workspace WORKSPACE --export-path EXPORT_PATH --team TEAM
```

If you do not want all projects you can filter project names:

```
asana-exporter --token TOKEN --workspace WORKSPACE --export-path EXPORT_PATH --team TEAM --project-filter "My\s+.+roject"
```

Once complete, your data will be under EXPORT_PATH and you can query it e.g.

List all teams found:

```
asana-exporter --export-path EXPORT_PATH --list-teams
```

List all extracted projects for a given team:

```
asana-exporter --token TOKEN --workspace WORKSPACE --export-path EXPORT_PATH --team "My Team" --list-projects
```

List all extracted tasks for a given project:

```
asana-exporter --token TOKEN --workspace WORKSPACE --export-path EXPORT_PATH --team "My Team" --list-project-tasks "My Project"
```

## MCP Server

An [MCP](https://modelcontextprotocol.io/) server is included that exposes the exported Asana archive to AI assistants like Claude. It provides read-only search and browse tools over the SQLite database.

### Prerequisites

Export your Asana data and import it into SQLite first using `asana-exporter`. The MCP server reads from the database at `~/.local/share/asana/asana.db` by default. Set the `ASANA_DB_PATH` environment variable to use a different path.

### Configuration

Add to your Claude Code settings (`.claude/settings.json`) or Claude Desktop config:

```json
{
  "mcpServers": {
    "asana-archive": {
      "command": "asana-mcp",
      "env": {
        "ASANA_DB_PATH": "/path/to/asana.db"
      }
    }
  }
}
```

If installed with `uv`, use the full path or `uvx`:

```json
{
  "mcpServers": {
    "asana-archive": {
      "command": "uvx",
      "args": ["--from", "asana-exporter", "asana-mcp"]
    }
  }
}
```

### Available Tools

| Tool | Description |
|------|-------------|
| `search_objects` | Quick typeahead search across projects, tasks, users, and teams |
| `search_tasks` | Full-text search across task names, notes, assignees, and tags |
| `get_task` | Get full task details including subtasks and stories |
| `get_tasks` | List tasks filtered by project, section, or assignee |
| `get_project` | Get project details including sections |
| `get_projects` | List projects with optional team and archived filters |
| `get_user` | Look up a user by GID or email |


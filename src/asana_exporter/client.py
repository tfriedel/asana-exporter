#!/usr/bin/env python3
import abc
import argparse
import collections
import concurrent.futures
import datetime
import json
import queue
import re
import sys
import threading
import time
import urllib
import urllib.request
from functools import cached_property
from pathlib import Path
from typing import Any

import asana
from loguru import logger

from asana_exporter import utils
from asana_exporter.utils import LOG


class AsanaResourceBase(abc.ABC):
    def __init__(self, force_update: bool = False) -> None:
        self.force_update = force_update
        self.export_semaphone = threading.Semaphore()

    @property
    @abc.abstractmethod
    def _local_store(self) -> Path:
        """Path to local store of information. If this exists it suggests we have
        done at least one api request for this resources.
        """

    @property
    @abc.abstractmethod
    def stats(self) -> "ExtractorStats":
        """Return ExtractorStats object."""

    @abc.abstractmethod
    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]] | None:
        """Fetch resources from the API."""

    @abc.abstractmethod
    def _from_local(self) -> list[dict[str, Any]] | None:
        """Fetch resources from the local cache/export."""

    @utils.with_lock
    def _export_write_locked(self, path: str | Path, data: str) -> None:
        p = Path(path)
        if not p.parent.is_dir():
            LOG.debug(f"write path not found {p.parent}")
            return

        p.write_text(data)

    @utils.with_lock
    def _export_read_locked(self, path: str | Path) -> str | None:
        """Read from export."""
        p = Path(path)
        if not p.exists():
            LOG.debug(f"read path not found {p}")
            return None

        return p.read_text()

    def get(
        self,
        update_from_api: bool = True,
        prefer_cache: bool = True,
        readonly: bool = False,
    ) -> list[dict[str, Any]]:
        if prefer_cache and not self.force_update:
            while self.export_semaphone._value == 0:
                update_from_api = False
                time.sleep(1)

            objs = self._from_local()
        else:
            objs = None

        if objs is None and (update_from_api or self.force_update):
            with self.export_semaphone:
                objs = self._from_api(readonly=readonly)

        if objs is None and not prefer_cache:
            while self.export_semaphone._value == 0:
                time.sleep(1)

            objs = self._from_local()

        return objs if objs is not None else []


class ExtractorStats(collections.UserDict):
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def combine(self, stats: dict[str, Any]) -> None:
        for key, value in stats.items():
            if key in self.data:
                if isinstance(value, dict):
                    self.data[key].update(value)
                else:
                    self.data[key] += value
            else:
                self.data[key] = value

    def __repr__(self) -> str:
        msg = []
        for key, value in self.items():
            msg.append(f"{key}={value}")

        return ", ".join(msg)


class AsanaProjects(AsanaResourceBase):
    def __init__(
        self,
        client: asana.ApiClient,
        workspace: str | int,
        team: dict[str, Any],
        project_include_filter: str | None,
        project_exclude_filter: str | None,
        projects_json: Path,
        force_update: bool,
    ) -> None:
        self.client = client
        self.workspace = workspace
        self.project_include_filter = project_include_filter
        self.project_exclude_filter = project_exclude_filter
        self.projects_json = projects_json
        self.team = team
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self) -> ExtractorStats:
        return self._stats

    @property
    def _local_store(self) -> Path:
        return self.projects_json

    def _filter_projects(self, projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        filtered = []
        for p in projects:
            if self.project_include_filter and not re.search(
                self.project_include_filter, p["name"]
            ):
                continue

            if self.project_exclude_filter and re.search(self.project_exclude_filter, p["name"]):
                continue

            filtered.append(p)

        return filtered

    def _from_local(self) -> list[dict[str, Any]] | None:
        LOG.debug(f"fetching projects for team '{self.team['name']}' from cache")
        projects = self._export_read_locked(self._local_store)
        if projects is None:
            return None

        filtered = self._filter_projects(json.loads(projects))
        self.stats["num_projects"] = len(filtered)
        return filtered

    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]]:
        LOG.info(f"fetching projects for team '{self.team['name']}' from api")
        total = 0
        ignored = []
        projects: list[dict[str, Any]] = []
        for p in asana.ProjectsApi(self.client).get_projects_for_team(self.team["gid"], {}):
            total += 1
            projects.append(p)

        all_projects = projects
        projects = self._filter_projects(projects)
        ignored = [p["name"] for p in all_projects if p not in projects]
        if ignored:
            LOG.info(f"ignoring projects:\n  {'\n  '.join(ignored)}")
            self.stats["num_projects_ignored"] = len(ignored)

        if readonly:
            return projects

        # yield
        time.sleep(0)

        LOG.debug(f"saving {len(projects)}/{total} projects")
        if projects:
            LOG.debug("updating project cache")
            self._export_write_locked(self._local_store, json.dumps(projects))

        self.stats["num_projects"] = len(projects)
        return projects


class AsanaProjectTasks(AsanaResourceBase):
    def __init__(
        self,
        client: asana.ApiClient,
        project: dict[str, Any],
        projects_dir: Path,
        force_update: bool = False,
    ) -> None:
        self.client = client
        self.project = project
        self.root_path = Path(projects_dir) / project["gid"]
        self._stats = ExtractorStats()
        self.modified_gids: set[str] | None = None  # None = full sync, set() = incremental
        super().__init__(force_update)

    @property
    def stats(self) -> ExtractorStats:
        return self._stats

    @property
    def _local_store(self) -> Path:
        return self.root_path / "tasks.json"

    @property
    def _sync_meta_path(self) -> Path:
        return self.root_path / ".sync_meta.json"

    def _load_sync_meta(self) -> dict[str, Any] | None:
        data = self._export_read_locked(self._sync_meta_path)
        if data is None:
            return None
        try:
            return json.loads(data)
        except (json.JSONDecodeError, KeyError):
            LOG.warning("corrupt sync metadata, will do full sync")
            return None

    def _save_sync_meta(self, timestamp: str) -> None:
        meta = {"last_sync": timestamp, "version": 1}
        self._export_write_locked(self._sync_meta_path, json.dumps(meta))

    def _from_local(self) -> list[dict[str, Any]] | None:
        p_name = self.project["name"]
        p_gid = self.project["gid"]
        LOG.debug(f"fetching tasks for project '{p_name}' (gid={p_gid}) from cache")
        tasks = self._export_read_locked(self._local_store)
        if tasks is None:
            return None

        tasks = json.loads(tasks)
        LOG.debug(f"fetched {len(tasks)} tasks")
        self.stats["num_tasks"] = len(tasks)
        return tasks

    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]]:
        p_name = self.project["name"]
        p_gid = self.project["gid"]
        LOG.info(f"fetching tasks for project '{p_name}' (gid={p_gid}) from api")
        path = self.root_path / "tasks"
        path.mkdir(parents=True, exist_ok=True)

        # Check if incremental sync is possible
        sync_meta = self._load_sync_meta()
        existing_tasks = self._from_local()

        use_incremental = (
            sync_meta is not None and existing_tasks is not None and not self.force_update
        )

        # Record sync start BEFORE API calls to avoid gaps
        sync_start = datetime.datetime.now(datetime.UTC).isoformat()

        if use_incremental:
            last_sync = sync_meta["last_sync"]
            LOG.info(f"incremental sync since {last_sync}")
            modified_tasks = []
            for t in asana.TasksApi(self.client).get_tasks(
                {"project": self.project["gid"], "modified_since": last_sync}
            ):
                modified_tasks.append(t)

            self.modified_gids = set(t["gid"] for t in modified_tasks)
            LOG.info(f"found {len(modified_tasks)} modified tasks")

            if not modified_tasks:
                if not readonly:
                    self._save_sync_meta(sync_start)
                self.stats["num_tasks"] = len(existing_tasks)
                return existing_tasks

            # Merge modified tasks into existing
            existing_by_gid = {t["gid"]: t for t in existing_tasks}

            # yield
            time.sleep(0)
            for t in modified_tasks:
                task_path = path / t["gid"]
                task_json_path = task_path / "task.json"
                task_path.mkdir(parents=True, exist_ok=True)

                fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
                self._export_write_locked(task_json_path, json.dumps(fulltask))
                existing_by_gid[t["gid"]] = fulltask

            tasks = list(existing_by_gid.values())
        else:
            # Full sync (first run or --force-update)
            self.modified_gids = None

            tasks = []
            for t in asana.TasksApi(self.client).get_tasks_for_project(self.project["gid"], {}):
                tasks.append(t)

            # yield
            time.sleep(0)
            for t in tasks:
                task_path = path / t["gid"]
                task_json_path = task_path / "task.json"
                if task_json_path.exists():
                    continue

                task_path.mkdir(parents=True, exist_ok=True)

                fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
                self._export_write_locked(task_json_path, json.dumps(fulltask))

        if not readonly:
            self._export_write_locked(self._local_store, json.dumps(tasks))
            self._save_sync_meta(sync_start)

        self.stats["num_tasks"] = len(tasks)
        return tasks


class AsanaTaskSubTasks(AsanaResourceBase):
    def __init__(
        self,
        client: asana.ApiClient,
        project: dict[str, Any],
        projects_dir: Path,
        task: dict[str, Any],
        force_update: bool = False,
    ) -> None:
        self.client = client
        self.project = project
        self.task = task
        self.root_path = Path(projects_dir) / project["gid"] / "tasks" / task["gid"]
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self) -> ExtractorStats:
        return self._stats

    @property
    def _local_store(self) -> Path:
        return self.root_path / "subtasks.json"

    def _from_local(self) -> list[dict[str, Any]] | None:
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.debug(f"fetching subtasks for task '{t_name}' (project={p_gid}) from cache")
        subtasks = self._export_read_locked(self._local_store)
        if subtasks is None:
            return None

        subtasks = json.loads(subtasks)
        self.stats["num_subtasks"] = len(subtasks)
        return subtasks

    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]]:
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.info(f"fetching subtasks for task='{t_name}' (project gid={p_gid}) from api")
        path = self.root_path / "subtasks"
        path.mkdir(parents=True, exist_ok=True)

        subtasks = []
        for s in asana.TasksApi(self.client).get_subtasks_for_task(self.task["gid"], {}):
            subtasks.append(s)

        # yield
        time.sleep(0)
        for t in subtasks:
            task_path = path / t["gid"]
            task_json_path = task_path / "subtask.json"
            if task_json_path.exists():
                continue

            task_path.mkdir(parents=True, exist_ok=True)

            fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
            self._export_write_locked(task_json_path, json.dumps(fulltask))

        if not readonly:
            self._export_write_locked(self._local_store, json.dumps(subtasks))

        self.stats["num_subtasks"] = len(subtasks)
        return subtasks


class AsanaProjectTaskStories(AsanaResourceBase):
    def __init__(
        self,
        client: asana.ApiClient,
        project: dict[str, Any],
        projects_dir: Path,
        task: dict[str, Any],
        force_update: bool = False,
    ) -> None:
        self.client = client
        self.project = project
        self.task = task
        self.root_path = Path(projects_dir) / project["gid"] / "tasks" / task["gid"]
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self) -> ExtractorStats:
        return self._stats

    @property
    def _local_store(self) -> Path:
        return self.root_path / "stories.json"

    def _from_local(self) -> list[dict[str, Any]] | None:
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.debug(f"fetching stories for task '{t_name}' (project={p_gid}) from cache")
        stories = self._export_read_locked(self._local_store)
        if stories is None:
            return None

        stories = json.loads(stories)
        self.stats["num_stories"] = len(stories)
        return stories

    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]]:
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.info(f"fetching stories for task='{t_name}' (project gid={p_gid}) from api")
        path = self.root_path / "stories"
        path.mkdir(parents=True, exist_ok=True)

        stories = []
        for s in asana.StoriesApi(self.client).get_stories_for_task(self.task["gid"], {}):
            stories.append(s)

        # yield
        time.sleep(0)
        for s in stories:
            self._export_write_locked(path / s["gid"], json.dumps(s))

        if not readonly:
            self._export_write_locked(self._local_store, json.dumps(stories))
        self.stats["num_stories"] = len(stories)
        return stories


class AsanaProjectTaskAttachments(AsanaResourceBase):
    def __init__(
        self,
        client: asana.ApiClient,
        project: dict[str, Any],
        projects_dir: Path,
        task: dict[str, Any],
        subtask: dict[str, Any] | None = None,
        force_update: bool = False,
    ) -> None:
        self.client = client
        self.project = project
        self._task = task
        self._subtask = subtask
        self.root_path = Path(projects_dir) / project["gid"] / "tasks" / task["gid"]
        if subtask:
            self.root_path = self.root_path / "subtasks" / subtask["gid"]
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self) -> ExtractorStats:
        return self._stats

    @property
    def task(self) -> dict[str, Any]:
        if self._subtask:
            return self._subtask

        return self._task

    @property
    def _local_store(self) -> Path:
        return self.root_path / "attachments.json"

    def _download(self, url: str, path: str | Path, retries: int = 3) -> None:
        LOG.debug(f"downloading {url} to {path}")
        for attempt in range(retries):
            try:
                rq = urllib.request.Request(url=url)
                with urllib.request.urlopen(rq) as url_fd, Path(path).open("wb") as fd:
                    out = "SOF"
                    while out:
                        out = url_fd.read(4096)
                        fd.write(out)
                return
            except (urllib.error.URLError, ConnectionError, OSError) as e:
                if attempt < retries - 1:
                    wait = 2**attempt
                    LOG.warning(
                        f"download failed (attempt {attempt + 1}/{retries}): {e}. "
                        f"Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    LOG.error(f"download failed after {retries} attempts: {e}")

    def _from_local(self) -> list[dict[str, Any]] | None:
        attachments = []
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        t_type = ""
        if self._subtask:
            t_type = "sub"

        LOG.debug(f"fetching attachments for {t_type}task '{t_name}' (project={p_gid}) from cache")
        raw = self._export_read_locked(self._local_store)
        if raw is None:
            return None

        attachments = json.loads(raw)
        self.stats["num_attachments"] = len(attachments)
        return attachments

    def _from_api(self, readonly: bool = False) -> list[dict[str, Any]]:
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        t_type = ""
        if self._subtask:
            t_type = "sub"

        LOG.info(f"fetching attachments for {t_type}task='{t_name}' (project gid={p_gid}) from api")
        path = self.root_path / "attachments"
        path.mkdir(parents=True, exist_ok=True)

        compact_attachments = []
        for s in asana.AttachmentsApi(self.client).get_attachments_for_object(self.task["gid"], {}):
            compact_attachments.append(s)

        # yield
        time.sleep(0)
        attachments = []
        for s in compact_attachments:
            # convert "compact" record to "full"
            full = asana.AttachmentsApi(self.client).get_attachment(s["gid"], {})
            with (path / s["gid"]).open("w") as fd:
                fd.write(json.dumps(full))
            url = full["download_url"]
            if url:
                self._download(url, path / f"{full['gid']}_download")
            attachments.append(full)

        if not readonly:
            self._export_write_locked(self._local_store, json.dumps(attachments))
        self.stats["num_attachments"] = len(attachments)
        return attachments


class AsanaExtractor:
    def __init__(
        self,
        token: str | None,
        workspace: str | None,
        teamname: str | None,
        export_path: str,
        project_include_filter: str | None = None,
        project_exclude_filter: str | None = None,
        force_update: bool = False,
    ) -> None:
        self._workspace = workspace
        self.teamname = teamname
        self.export_path = export_path
        self.project_include_filter = project_include_filter
        self.project_exclude_filter = project_exclude_filter
        self.token = token
        self.force_update = force_update
        Path(self.export_path).mkdir(parents=True, exist_ok=True)

    @cached_property
    @utils.required({"token": "--token"})
    def client(self) -> asana.ApiClient:
        configuration = asana.Configuration()
        configuration.access_token = self.token
        api_client = asana.ApiClient(configuration)
        api_client.default_headers["Asana-Enable"] = "new_user_task_lists"
        return api_client

    @cached_property
    @utils.required({"_workspace": "--workspace"})
    def workspace(self) -> int | str:
        """If workspace name is provided we need to lookup its GID."""
        try:
            return int(self._workspace)
        except ValueError:
            ws = next(
                (
                    w["gid"]
                    for w in asana.WorkspacesApi(self.client).get_workspaces({})
                    if w["name"] == self._workspace
                ),
                None,
            )
            if ws is None:
                msg = f"No workspace found with name '{self._workspace}'"
                raise ValueError(msg) from None
            LOG.info(f"resolved workspace name '{self._workspace}' to gid '{ws}'")
            return ws

    @property
    def teams_json(self) -> Path:
        return Path(self.export_path) / "teams.json"

    @property
    def global_sync_meta_path(self) -> Path:
        """Path to the workspace-level sync metadata file."""
        return Path(self.export_path) / ".global_sync_meta.json"

    def load_global_sync_meta(self) -> dict[str, Any] | None:
        """Load workspace-level sync metadata.

        Returns:
            Parsed metadata dict if valid, or None if the file does not
            exist or contains corrupt/incomplete data.
        """
        path = self.global_sync_meta_path
        if not path.exists():
            return None
        try:
            meta = json.loads(path.read_text())
        except json.JSONDecodeError:
            LOG.warning("corrupt global sync metadata, will do full sync")
            return None
        if "last_sync" not in meta:
            LOG.warning("global sync metadata missing last_sync, will do full sync")
            return None
        return meta

    def save_global_sync_meta(self, timestamp: str) -> None:
        """Save workspace-level sync metadata.

        Args:
            timestamp: ISO 8601 timestamp of the sync start time.
        """
        meta = {"last_sync": timestamp, "version": 1}
        self.global_sync_meta_path.write_text(json.dumps(meta))

    def search_modified_tasks_global(self) -> dict[str, set[str]] | None:
        """Check for workspace-wide changes using premium search API.

        Returns:
            dict mapping project GID → set of modified task GIDs, or
            None if full sync is needed (first run, non-premium, or error).
        """
        meta = self.load_global_sync_meta()
        if meta is None:
            LOG.info("no global sync metadata, will do full sync")
            return None

        last_sync = meta["last_sync"]
        LOG.info(f"pre-flight: checking workspace for changes since {last_sync}")
        try:
            results = list(
                asana.TasksApi(self.client).search_tasks_for_workspace(
                    str(self.workspace),
                    {
                        "modified_at.after": last_sync,
                        "opt_fields": "memberships.project.gid",
                    },
                )
            )
        except asana.rest.ApiException as e:
            if e.status == 402:
                LOG.warning(
                    "search_tasks_for_workspace requires premium, "
                    "falling back to per-project sync"
                )
                return None
            raise

        modified_by_project: dict[str, set[str]] = {}
        for task in results:
            task_gid = task["gid"]
            memberships = task.get("memberships", [])
            for m in memberships:
                project = m.get("project") or {}
                project_gid = project.get("gid")
                if project_gid:
                    modified_by_project.setdefault(project_gid, set()).add(task_gid)

        LOG.info(
            f"pre-flight: {len(results)} modified tasks across "
            f"{len(modified_by_project)} projects"
        )
        return modified_by_project

    @utils.with_lock
    def get_teams(self, readonly: bool = False) -> tuple[list[dict[str, Any]], ExtractorStats]:
        stats = ExtractorStats()
        teams: list[dict[str, Any]] = []
        if self.teams_json.exists():
            LOG.debug("using cached teams")
            teams = json.loads(self.teams_json.read_text())
        else:
            LOG.debug("fetching teams from api")
            teams = []
            for p in asana.TeamsApi(self.client).get_teams_for_workspace(str(self.workspace), {}):
                teams.append(p)

            if not readonly:
                self.teams_json.write_text(json.dumps(teams))

                LOG.debug(f"saved {len(teams)} teams")

        stats["num_teams"] = len(teams)
        return teams, stats

    @cached_property
    @utils.required({"token": "--token", "_workspace": "--workspace", "teamname": "--team"})
    def team(self) -> dict[str, Any]:
        teams, _ = self.get_teams()
        for t in teams:
            if t["name"] == self.teamname:
                return t

        raise Exception(f"No team found with name '{self.teamname}'")

    @property
    def export_path_team(self) -> Path:
        return Path(self.export_path) / "teams" / self.team["gid"]

    @property
    def projects_json(self) -> Path:
        return self.export_path_team / "projects.json"

    @property
    def projects_dir(self) -> Path:
        return self.export_path_team / "projects"

    @utils.required({"teamname": "--team"})
    def get_projects(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], ExtractorStats]:
        """Fetch projects owned by a give team. By default this will get projects
        from the Asana api and add them to the extraction archive.

        Since the api can be slow, it is recommended to use the available
        project filters.
        """
        ap = AsanaProjects(
            self.client,
            self.workspace,
            self.team,
            self.project_include_filter,
            self.project_exclude_filter,
            self.projects_json,
            force_update=self.force_update,
        )
        projects = ap.get(*args, **kwargs)
        return projects, ap.stats

    def get_project_templates(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        root_path = self.projects_dir
        templates_json = root_path / "templates.json"
        if templates_json.exists():
            LOG.debug("using cached project templates")
            templates = json.loads(templates_json.read_text())
        else:
            LOG.info(f"fetching project templates for team '{self.team['name']}' from api")
            templates_dir = root_path / "templates"
            templates_dir.mkdir(parents=True, exist_ok=True)

            templates = []
            for t in asana.ProjectsApi(self.client).get_projects_for_team(
                self.team["gid"], {"is_template": True}
            ):
                templates.append(t)

            # yield
            time.sleep(0)

            templates_json.write_text(json.dumps(templates))

        return templates, {}

    def get_project_tasks(
        self, project: dict[str, Any], *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], ExtractorStats, set[str] | None]:
        """@param update_from_api: allow fetching from the API."""
        apt = AsanaProjectTasks(
            self.client, project, self.projects_dir, force_update=self.force_update
        )
        tasks = apt.get(*args, **kwargs)
        return tasks, apt.stats, apt.modified_gids

    def get_task_subtasks(
        self,
        project: dict[str, Any],
        task: dict[str, Any],
        event: threading.Event,
        stats_queue: queue.Queue[ExtractorStats],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """@param update_from_api: allow fetching from the API."""
        event.clear()
        atst = AsanaTaskSubTasks(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        atst.get(*args, **kwargs)
        # notify any waiting threads that subtasks are now available.
        event.set()
        stats_queue.put(atst.stats)

    def get_task_stories(
        self,
        project: dict[str, Any],
        task: dict[str, Any],
        stats_queue: queue.Queue[ExtractorStats],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """@param update_from_api: allow fetching from the API."""
        ats = AsanaProjectTaskStories(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        ats.get(*args, **kwargs)
        stats_queue.put(ats.stats)

    def get_task_attachments(
        self,
        project: dict[str, Any],
        task: dict[str, Any],
        stats_queue: queue.Queue[ExtractorStats],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """@param update_from_api: allow fetching from the API."""
        ata = AsanaProjectTaskAttachments(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        ata.get(*args, **kwargs)
        stats_queue.put(ata.stats)

    def get_subtask_attachments(
        self,
        project: dict[str, Any],
        task: dict[str, Any],
        event: threading.Event,
        stats_queue: queue.Queue[ExtractorStats],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """@param update_from_api: allow fetching from the API."""
        while not event.is_set():
            time.sleep(1)

        # Subtasks were already fetched by get_task_subtasks, so always
        # read from cache here (no need to re-fetch from API)
        st_obj = AsanaTaskSubTasks(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        subtasks = st_obj.get(update_from_api=False)
        stats = st_obj.stats
        stats["num_subtask_attachments"] = 0
        if not subtasks:
            LOG.debug("no subtasks to fetch attachments for")
            stats_queue.put(stats)
            return

        attachments = []
        LOG.debug(f"fetching attachments for {len(subtasks)} subtasks")
        for subtask in subtasks:
            asta = AsanaProjectTaskAttachments(
                self.client,
                project,
                self.projects_dir,
                task,
                subtask,
                force_update=self.force_update,
            )
            # Pass through kwargs (e.g. prefer_cache=False) for attachments
            attachments.extend(asta.get(*args, **kwargs))
            stats["num_subtask_attachments"] += asta.stats["num_attachments"]

        stats_queue.put(stats)

    def run(self, changed_project_gids: set[str] | None = None) -> None:
        """Extract projects, tasks, and sub-resources for this team.

        Args:
            changed_project_gids: If provided, only sync projects whose GID is
                in this set.  None means sync all projects.
        """
        LOG.info("=" * 80)
        LOG.info(f"starting extraction to {self.export_path}")
        start = time.time()

        self.export_path_team.mkdir(parents=True, exist_ok=True)

        self.get_project_templates()
        projects, stats = self.get_projects(prefer_cache=False)
        stats_queue: queue.Queue[ExtractorStats] = queue.Queue()

        # Filter to only changed projects when pre-flight data is available
        if changed_project_gids is not None:
            projects_to_sync = [p for p in projects if p["gid"] in changed_project_gids]
            if len(projects_to_sync) < len(projects):
                LOG.info(
                    f"pre-flight: syncing {len(projects_to_sync)}/{len(projects)} "
                    f"changed projects"
                )
        else:
            projects_to_sync = projects

        # Phase 1: Fetch tasks for all projects concurrently
        project_results: list[
            tuple[dict[str, Any], list[dict[str, Any]], ExtractorStats, set[str] | None]
        ] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_project = {
                executor.submit(self.get_project_tasks, p, prefer_cache=False): p
                for p in projects_to_sync
            }
            for future in concurrent.futures.as_completed(future_to_project):
                p = future_to_project[future]
                try:
                    tasks, task_stats, modified_gids = future.result()
                except Exception:
                    LOG.exception(f"failed to fetch tasks for project '{p['name']}', skipping")
                    continue
                project_results.append((p, tasks, task_stats, modified_gids))

        # Phase 2: Process sub-resources for projects with modified tasks
        for p, tasks, task_stats, modified_gids in project_results:
            LOG.info(f"extracting project {p['name']}")
            stats.combine(task_stats)

            # Determine which tasks need sub-resource refresh
            if modified_gids is not None:
                tasks_to_process = [t for t in tasks if t["gid"] in modified_gids]
                LOG.info(
                    f"processing {len(tasks_to_process)}/{len(tasks)} modified tasks for project "
                    f"'{p['name']}'"
                )
            else:
                tasks_to_process = tasks

            if not tasks_to_process:
                continue

            # For modified tasks in incremental mode, bypass sub-resource
            # cache to pick up changes
            force_sub = modified_gids is not None

            jobs = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                for t in tasks_to_process:
                    jobs[
                        executor.submit(
                            self.get_task_stories, p, t, stats_queue, prefer_cache=not force_sub
                        )
                    ] = t["name"]

                    jobs[
                        executor.submit(
                            self.get_task_attachments, p, t, stats_queue, prefer_cache=not force_sub
                        )
                    ] = t["name"]
                    event = threading.Event()
                    jobs[
                        executor.submit(
                            self.get_task_subtasks,
                            p,
                            t,
                            event,
                            stats_queue,
                            prefer_cache=not force_sub,
                        )
                    ] = t["name"]
                    jobs[
                        executor.submit(
                            self.get_subtask_attachments,
                            p,
                            t,
                            event,
                            stats_queue,
                            prefer_cache=not force_sub,
                        )
                    ] = t["name"]

                for job in concurrent.futures.as_completed(jobs):
                    job.result()

        LOG.info(f"completed extraction of {len(projects_to_sync)} projects.")
        while not stats_queue.empty():
            item = stats_queue.get()
            stats.combine(item)

        LOG.info(f"stats: {stats}")
        end = time.time()
        LOG.info(f"extraction to {self.export_path} completed in {round(end - start, 3)} secs.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", default=False, help=("enable debug logging")
    )
    parser.add_argument("--token", type=str, default=None, help="Asana api token")
    parser.add_argument(
        "--workspace",
        type=str,
        default=None,
        help=(
            "Asana workspace ID or name. If a "
            "name is provided an api lookup "
            "is done to resolve the ID."
        ),
    )
    parser.add_argument("--team", type=str, default=None, help="Asana team name")
    parser.add_argument(
        "--export-path",
        type=str,
        default="asana_export",
        required=False,
        help="Path where data is saved.",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        default=False,
        required=False,
        help="Force updates to existing resources.",
    )
    parser.add_argument(
        "--exclude-projects",
        type=str,
        default=None,
        help=("Regular expression filter used to exclude projects."),
    )
    parser.add_argument(
        "--project-filter",
        type=str,
        default=None,
        help=("Regular expression filter used to include projects."),
    )
    parser.add_argument(
        "--all-teams",
        action="store_true",
        default=False,
        help="Export all teams (instead of specifying --team).",
    )
    parser.add_argument(
        "--list-workspaces",
        action="store_true",
        default=False,
        help="List all workspaces accessible with the given token.",
    )
    parser.add_argument(
        "--list-teams",
        action="store_true",
        default=False,
        help=(
            "List all teams. This will "
            "return teams retrieved from an existing "
            "extraction archive and if not available will "
            "query the api."
        ),
    )
    parser.add_argument(
        "--list-projects",
        action="store_true",
        default=False,
        help=(
            "List all cached projects. This will only "
            "return projects retrieved from an existing "
            "extraction archive and will not perform any "
            "api calls."
        ),
    )
    parser.add_argument(
        "--list-project-tasks",
        type=str,
        default=None,
        help=(
            "List all cached tasks for a given project. "
            "This will only "
            "return project tasks retrieved from an "
            "existing "
            "extraction archive and will not perform any "
            "api calls."
        ),
    )
    parser.add_argument(
        "--to-sqlite",
        action="store_true",
        default=False,
        help="Import exported JSON files into a SQLite database. No API calls are made.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="SQLite database path (default: <export-path>/asana.db). Used with --to-sqlite.",
    )

    args = parser.parse_args()
    if args.debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    if args.to_sqlite:
        import sqlite3

        from asana_exporter.database import (
            import_export_dir,
            migrate_schema,
            rebuild_task_search_fts,
        )

        if not Path(args.export_path).is_dir():
            LOG.error(f"export path '{args.export_path}' does not exist or is not a directory")
            return

        db_path = args.db or str(Path(args.export_path) / "asana.db")
        LOG.info(f"importing JSON from '{args.export_path}' into '{db_path}'")
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            migrate_schema(conn)
            stats = import_export_dir(conn, args.export_path, force=args.force_update)
            fts_count = rebuild_task_search_fts(conn)
            LOG.info(f"import complete: {stats}")
            LOG.info(f"FTS index: {fts_count} top-level tasks indexed")
        finally:
            conn.close()

        LOG.info("done.")
        return

    ae = AsanaExtractor(
        token=args.token,
        workspace=args.workspace,
        teamname=args.team,
        export_path=args.export_path,
        project_include_filter=args.project_filter,
        project_exclude_filter=args.exclude_projects,
        force_update=args.force_update,
    )
    if args.list_workspaces:
        configuration = asana.Configuration()
        configuration.access_token = args.token
        api_client = asana.ApiClient(configuration)
        workspaces = list(asana.WorkspacesApi(api_client).get_workspaces({}))
        if workspaces:
            print("\nWorkspaces:")
            print("\n".join([f"{w['gid']}: '{w['name']}'" for w in workspaces]))
    elif args.list_teams:
        teams, stats = ae.get_teams(readonly=True)
        LOG.debug(f"stats: {stats}")
        if teams:
            print("\nTeams:")
            print("\n".join([f"{t['gid']}: '{t['name']}'" for t in teams]))
    elif args.list_projects:
        projects, stats = ae.get_projects(prefer_cache=True, readonly=True)
        LOG.debug(f"stats: {stats}")
        if projects:
            print("\nProjects:")
            print("\n".join([f"{p['gid']}: '{p['name']}'" for p in projects]))
    elif args.list_project_tasks:
        projects, stats = ae.get_projects(update_from_api=False, readonly=True)
        LOG.debug(f"stats: {stats}")
        for p in projects:
            if p["name"] == args.list_project_tasks:
                tasks, _, _ = ae.get_project_tasks(p, update_from_api=False, readonly=True)
                if tasks:
                    print("\nTasks:")
                    print("\n".join([f"{t['gid']}: '{t['name']}'" for t in tasks]))

                break
        else:
            pnames = [f"'{p['name']}'" for p in projects]
            LOG.debug(
                f"project name '{args.list_project_tasks}' does not match any of the following:"
                f"\n{'\n'.join(pnames)}"
            )
    elif args.all_teams:
        teams, _ = ae.get_teams()
        workspace_gid = str(ae.workspace)  # Resolve once, reuse for all teams
        sync_start = datetime.datetime.now(datetime.UTC).isoformat()

        # Pre-flight: one API call to find all workspace-wide changes
        if not args.force_update:
            modified_by_project = ae.search_modified_tasks_global()
        else:
            modified_by_project = None

        if modified_by_project is not None and len(modified_by_project) == 0:
            LOG.info("no workspace-wide changes since last sync, skipping all teams")
            ae.save_global_sync_meta(sync_start)
        else:
            changed_project_gids = (
                set(modified_by_project.keys()) if modified_by_project is not None else None
            )
            for t in teams:
                LOG.info(f"exporting team '{t['name']}'")
                team_ae = AsanaExtractor(
                    token=args.token,
                    workspace=workspace_gid,
                    teamname=t["name"],
                    export_path=args.export_path,
                    project_include_filter=args.project_filter,
                    project_exclude_filter=args.exclude_projects,
                    force_update=args.force_update,
                )
                team_ae.run(changed_project_gids=changed_project_gids)
            ae.save_global_sync_meta(sync_start)
    else:
        ae.run()

    LOG.info("done.")


if __name__ == "__main__":
    main()

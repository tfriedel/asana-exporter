#!/usr/bin/env python3
import abc
import argparse
import collections
import concurrent.futures
import datetime
import json
import os
import queue
import re
import sys
import threading
import time
import urllib
import urllib.request

from functools import cached_property

from loguru import logger

from asana_exporter import utils
from asana_exporter.utils import LOG

import asana


class AsanaResourceBase(abc.ABC):
    def __init__(self, force_update=False):
        self.force_update = force_update
        self.export_semaphone = threading.Semaphore()

    @property
    @abc.abstractmethod
    def _local_store(self):
        """
        Path to local store of information. If this exists it suggests we have
        done at least one api request for this resources.
        """

    @property
    @abc.abstractmethod
    def stats(self):
        """Return ExtractorStats object."""

    @abc.abstractmethod
    def _from_api(self, readonly=False):
        """Fetch resources from the API."""

    @abc.abstractmethod
    def _from_local(self):
        """Fetch resources from the local cache/export."""

    @utils.with_lock
    def _export_write_locked(self, path, data):
        if not os.path.isdir(os.path.dirname(path)):
            LOG.debug(f"write path not found {os.path.dirname(path)}")
            return

        with open(path, "w") as fd:
            fd.write(data)

    @utils.with_lock
    def _export_read_locked(self, path):
        """read from export"""
        if not os.path.exists(path):
            LOG.debug(f"read path not found {path}")
            return

        with open(path, "r") as fd:
            return fd.read()

    def get(self, update_from_api=True, prefer_cache=True, readonly=False):
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
    def __init__(self):
        self.data = {}

    def combine(self, stats):
        for key, value in stats.items():
            if key in self.data:
                if isinstance(value, dict):
                    self.data[key].update(value)
                else:
                    self.data[key] += value
            else:
                self.data[key] = value

    def __repr__(self):
        msg = []
        for key, value in self.items():
            msg.append(f"{key}={value}")

        return ", ".join(msg)


class AsanaProjects(AsanaResourceBase):
    def __init__(
        self,
        client,
        workspace,
        team,
        project_include_filter,
        project_exclude_filter,
        projects_json,
        force_update,
    ):
        self.client = client
        self.workspace = workspace
        self.project_include_filter = project_include_filter
        self.project_exclude_filter = project_exclude_filter
        self.projects_json = projects_json
        self.team = team
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self):
        return self._stats

    @property
    def _local_store(self):
        return self.projects_json

    def _filter_projects(self, projects):
        _projects = []
        for p in projects:
            if self.project_include_filter:
                if not re.search(self.project_include_filter, p["name"]):
                    continue

            if self.project_exclude_filter:
                if re.search(self.project_exclude_filter, p["name"]):
                    continue

            _projects.append(p)

        return _projects

    def _from_local(self):
        LOG.debug(f"fetching projects for team '{self.team['name']}' from cache")
        projects = self._export_read_locked(self._local_store)
        if projects is None:
            return None

        projects = self._filter_projects(json.loads(projects))
        self.stats["num_projects"] = len(projects)
        return projects

    def _from_api(self, readonly=False):
        LOG.info(f"fetching projects for team '{self.team['name']}' from api")
        total = 0
        ignored = []
        projects = []
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
            """
            if os.path.exists(self.projects_json):
                with open(self.projects_json) as fd:
                    _projects = json.loads(fd.read())

                for p in _projects:
                    if p not in projects:
                        projects.append(p)
            """

            self._export_write_locked(self._local_store, json.dumps(projects))

        self.stats["num_projects"] = len(projects)
        return projects


class AsanaProjectTasks(AsanaResourceBase):
    def __init__(self, client, project, projects_dir, force_update=False):
        self.client = client
        self.project = project
        self.root_path = os.path.join(projects_dir, project["gid"])
        self._stats = ExtractorStats()
        self.modified_gids = None  # None = full sync, set() = incremental
        super().__init__(force_update)

    @property
    def stats(self):
        return self._stats

    @property
    def _local_store(self):
        return os.path.join(self.root_path, "tasks.json")

    @property
    def _sync_meta_path(self):
        return os.path.join(self.root_path, ".sync_meta.json")

    def _load_sync_meta(self):
        data = self._export_read_locked(self._sync_meta_path)
        if data is None:
            return None
        try:
            return json.loads(data)
        except (json.JSONDecodeError, KeyError):
            LOG.warning("corrupt sync metadata, will do full sync")
            return None

    def _save_sync_meta(self, timestamp):
        meta = {"last_sync": timestamp, "version": 1}
        self._export_write_locked(self._sync_meta_path, json.dumps(meta))

    def _from_local(self):
        LOG.debug(
            f"fetching tasks for project '{self.project['name']}' (gid={self.project['gid']}) from cache"
        )
        tasks = self._export_read_locked(self._local_store)
        if tasks is None:
            return None

        tasks = json.loads(tasks)
        LOG.debug(f"fetched {len(tasks)} tasks")
        self.stats["num_tasks"] = len(tasks)
        return tasks

    def _from_api(self, readonly=False):
        LOG.info(
            f"fetching tasks for project '{self.project['name']}' (gid={self.project['gid']}) from api"
        )
        path = os.path.join(self.root_path, "tasks")
        if not os.path.isdir(path):
            os.makedirs(path)

        # Check if incremental sync is possible
        sync_meta = self._load_sync_meta()
        existing_tasks = self._from_local()

        use_incremental = (
            sync_meta is not None and existing_tasks is not None and not self.force_update
        )

        # Record sync start BEFORE API calls to avoid gaps
        sync_start = datetime.datetime.now(datetime.timezone.utc).isoformat()

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
                task_path = os.path.join(path, t["gid"])
                task_json_path = os.path.join(task_path, "task.json")
                if not os.path.isdir(task_path):
                    os.makedirs(task_path)

                fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
                self._export_write_locked(task_json_path, json.dumps(fulltask))
                existing_by_gid[t["gid"]] = t

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
                task_path = os.path.join(path, t["gid"])
                task_json_path = os.path.join(task_path, "task.json")
                if os.path.exists(task_json_path):
                    continue

                if not os.path.isdir(task_path):
                    os.makedirs(task_path)

                fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
                self._export_write_locked(task_json_path, json.dumps(fulltask))

        if not readonly:
            self._export_write_locked(self._local_store, json.dumps(tasks))
            self._save_sync_meta(sync_start)

        self.stats["num_tasks"] = len(tasks)
        return tasks


class AsanaTaskSubTasks(AsanaResourceBase):
    def __init__(self, client, project, projects_dir, task, force_update=False):
        self.client = client
        self.project = project
        self.task = task
        self.root_path = os.path.join(projects_dir, project["gid"], "tasks", task["gid"])
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self):
        return self._stats

    @property
    def _local_store(self):
        return os.path.join(self.root_path, "subtasks.json")

    def _from_local(self):
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.debug(f"fetching subtasks for task '{t_name}' (project={p_gid}) from cache")
        subtasks = self._export_read_locked(self._local_store)
        if subtasks is None:
            return None

        subtasks = json.loads(subtasks)
        self.stats["num_subtasks"] = len(subtasks)
        return subtasks

    def _from_api(self, readonly=False):
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.info(f"fetching subtasks for task='{t_name}' (project gid={p_gid}) from api")
        path = os.path.join(self.root_path, "subtasks")
        if not os.path.isdir(path):
            os.makedirs(path)

        subtasks = []
        for s in asana.TasksApi(self.client).get_subtasks_for_task(self.task["gid"], {}):
            subtasks.append(s)

        # yield
        time.sleep(0)
        for t in subtasks:
            task_path = os.path.join(path, t["gid"])
            task_json_path = os.path.join(task_path, "subtask.json")
            if os.path.exists(task_json_path):
                continue

            if not os.path.isdir(task_path):
                os.makedirs(task_path)

            fulltask = asana.TasksApi(self.client).get_task(t["gid"], {})
            self._export_write_locked(task_json_path, json.dumps(fulltask))

        self._export_write_locked(self._local_store, json.dumps(subtasks))

        self.stats["num_subtasks"] = len(subtasks)
        return subtasks


class AsanaProjectTaskStories(AsanaResourceBase):
    def __init__(self, client, project, projects_dir, task, force_update=False):
        self.client = client
        self.project = project
        self.task = task
        self.root_path = os.path.join(projects_dir, project["gid"], "tasks", task["gid"])
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self):
        return self._stats

    @property
    def _local_store(self):
        return os.path.join(self.root_path, "stories.json")

    def _from_local(self):
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.debug(f"fetching stories for task '{t_name}' (project={p_gid}) from cache")
        stories = self._export_read_locked(self._local_store)
        if stories is None:
            return None

        stories = json.loads(stories)
        self.stats["num_stories"] = len(stories)
        return stories

    def _from_api(self, readonly=False):
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        LOG.info(f"fetching stories for task='{t_name}' (project gid={p_gid}) from api")
        path = os.path.join(self.root_path, "stories")
        if not os.path.isdir(path):
            os.makedirs(path)

        stories = []
        for s in asana.StoriesApi(self.client).get_stories_for_task(self.task["gid"], {}):
            stories.append(s)

        # yield
        time.sleep(0)
        for s in stories:
            self._export_write_locked(os.path.join(path, s["gid"]), json.dumps(s))

        self._export_write_locked(self._local_store, json.dumps(stories))
        self.stats["num_stories"] = len(stories)
        return stories


class AsanaProjectTaskAttachments(AsanaResourceBase):
    def __init__(self, client, project, projects_dir, task, subtask=None, force_update=False):
        self.client = client
        self.project = project
        self._task = task
        self._subtask = subtask
        self.root_path = os.path.join(projects_dir, project["gid"], "tasks", task["gid"])
        if subtask:
            self.root_path = os.path.join(self.root_path, "subtasks", subtask["gid"])
        self._stats = ExtractorStats()
        super().__init__(force_update)

    @property
    def stats(self):
        return self._stats

    @property
    def task(self):
        if self._subtask:
            return self._subtask

        return self._task

    @property
    def _local_store(self):
        return os.path.join(self.root_path, "attachments.json")

    def _download(self, url, path, retries=3):
        LOG.debug(f"downloading {url} to {path}")
        for attempt in range(retries):
            try:
                rq = urllib.request.Request(url=url)
                with urllib.request.urlopen(rq) as url_fd:
                    with open(path, "wb") as fd:
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

    def _from_local(self):
        attachments = []
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        t_type = ""
        if self._subtask:
            t_type = "sub"

        LOG.debug(f"fetching attachments for {t_type}task '{t_name}' (project={p_gid}) from cache")
        attachments = self._export_read_locked(self._local_store)
        if attachments is None:
            return None

        attachments = json.loads(attachments)
        self.stats["num_attachments"] = len(attachments)
        return attachments

    def _from_api(self, readonly=False):
        p_gid = self.project["gid"].strip()
        t_name = self.task["name"].strip()
        t_type = ""
        if self._subtask:
            t_type = "sub"

        LOG.info(f"fetching attachments for {t_type}task='{t_name}' (project gid={p_gid}) from api")
        path = os.path.join(self.root_path, "attachments")
        if not os.path.isdir(path):
            os.makedirs(path)

        attachments = []
        for s in asana.AttachmentsApi(self.client).get_attachments_for_object(self.task["gid"], {}):
            attachments.append(s)

        # yield
        time.sleep(0)
        for s in attachments:
            # convert "compact" record to "full"
            with open(os.path.join(path, s["gid"]), "w") as fd:
                s = asana.AttachmentsApi(self.client).get_attachment(s["gid"], {})
                fd.write(json.dumps(s))
                url = s["download_url"]
                if url:
                    self._download(url, os.path.join(path, f"{s['gid']}_download"))

        self._export_write_locked(self._local_store, json.dumps(attachments))
        self.stats["num_attachments"] = len(attachments)
        return attachments


class AsanaExtractor:
    def __init__(
        self,
        token,
        workspace,
        teamname,
        export_path,
        project_include_filter=None,
        project_exclude_filter=None,
        force_update=False,
    ):
        self._workspace = workspace
        self.teamname = teamname
        self.export_path = export_path
        self.project_include_filter = project_include_filter
        self.project_exclude_filter = project_exclude_filter
        self.token = token
        self.force_update = force_update
        if not os.path.isdir(self.export_path):
            os.makedirs(self.export_path)

    @cached_property
    @utils.required({"token": "--token"})
    def client(self):
        configuration = asana.Configuration()
        configuration.access_token = self.token
        api_client = asana.ApiClient(configuration)
        api_client.default_headers["Asana-Enable"] = "new_user_task_lists"
        return api_client

    @cached_property
    @utils.required({"_workspace": "--workspace"})
    def workspace(self):
        """
        If workspace name is provided we need to lookup its GID.
        """
        try:
            return int(self._workspace)
        except ValueError:
            ws = [
                w["gid"]
                for w in asana.WorkspacesApi(self.client).get_workspaces({})
                if w["name"] == self._workspace
            ][0]
            LOG.info(f"resolved workspace name '{self._workspace}' to gid '{ws}'")
            return ws

    @property
    def teams_json(self):
        return os.path.join(self.export_path, "teams.json")

    @utils.with_lock
    def get_teams(self, readonly=False):
        stats = ExtractorStats()
        teams = []
        if os.path.exists(self.teams_json):
            LOG.debug("using cached teams")
            with open(self.teams_json) as fd:
                teams = json.loads(fd.read())
        else:
            LOG.debug("fetching teams from api")
            teams = []
            for p in asana.TeamsApi(self.client).get_teams_for_workspace(str(self.workspace), {}):
                teams.append(p)

            if not readonly:
                with open(self.teams_json, "w") as fd:
                    fd.write(json.dumps(teams))

                LOG.debug(f"saved {len(teams)} teams")

        stats["num_teams"] = len(teams)
        return teams, stats

    @cached_property
    @utils.required({"token": "--token", "_workspace": "--workspace", "teamname": "--team"})
    def team(self):
        teams, _ = self.get_teams()
        for t in teams:
            if t["name"] == self.teamname:
                return t

        raise Exception(f"No team found with name '{self.teamname}'")

    @property
    def export_path_team(self):
        return os.path.join(self.export_path, "teams", self.team["gid"])

    @property
    def projects_json(self):
        return os.path.join(self.export_path_team, "projects.json")

    @property
    def projects_dir(self):
        return os.path.join(self.export_path_team, "projects")

    @utils.required({"teamname": "--team"})
    def get_projects(self, *args, **kwargs):
        """
        Fetch projects owned by a give team. By default this will get projects
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

    def get_project_templates(self):
        root_path = path = os.path.join(self.projects_dir)
        if os.path.exists(os.path.join(root_path, "templates.json")):
            LOG.debug("using cached project templates")
            with open(os.path.join(root_path, "templates.json")) as fd:
                templates = json.loads(fd.read())
        else:
            LOG.info(f"fetching project templates for team '{self.team['name']}' from api")
            path = os.path.join(root_path, "templates")
            if not os.path.isdir(path):
                os.makedirs(path)

            templates = []
            for t in asana.ProjectsApi(self.client).get_projects_for_team(
                self.team["gid"], {"is_template": True}
            ):
                templates.append(t)

            # yield
            time.sleep(0)

            with open(os.path.join(root_path, "templates.json"), "w") as fd:
                fd.write(json.dumps(templates))

        return templates, {}

    def get_project_tasks(self, project, *args, **kwargs):
        """
        @param update_from_api: allow fetching from the API.
        """
        apt = AsanaProjectTasks(
            self.client, project, self.projects_dir, force_update=self.force_update
        )
        tasks = apt.get(*args, **kwargs)
        return tasks, apt.stats, apt.modified_gids

    def get_task_subtasks(self, project, task, event, stats_queue, *args, **kwargs):
        """
        @param update_from_api: allow fetching from the API.
        """
        event.clear()
        atst = AsanaTaskSubTasks(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        atst.get(*args, **kwargs)
        # notify any waiting threads that subtasks are now available.
        event.set()
        stats_queue.put(atst.stats)

    def get_task_stories(self, project, task, stats_queue, *args, **kwargs):
        """
        @param update_from_api: allow fetching from the API.
        """
        ats = AsanaProjectTaskStories(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        ats.get(*args, **kwargs)
        stats_queue.put(ats.stats)

    def get_task_attachments(self, project, task, stats_queue, *args, **kwargs):
        """
        @param update_from_api: allow fetching from the API.
        """
        ata = AsanaProjectTaskAttachments(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        ata.get(*args, **kwargs)
        stats_queue.put(ata.stats)

    def get_subtask_attachments(self, project, task, event, stats_queue, *args, **kwargs):
        """
        @param update_from_api: allow fetching from the API.
        """
        while not event.is_set():
            time.sleep(1)

        # Subtasks were already fetched by get_task_subtasks, so always
        # read from cache here (no need to re-fetch from API)
        st_obj = AsanaTaskSubTasks(
            self.client, project, self.projects_dir, task, force_update=self.force_update
        )
        subtasks = st_obj.get()
        stats = st_obj.stats
        stats["num_subtask_attachments"] = 0
        if not subtasks:
            LOG.debug("no subtasks to fetch attachments for")
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

    def run(self):
        LOG.info("=" * 80)
        LOG.info(f"starting extraction to {self.export_path}")
        start = time.time()

        if not os.path.isdir(self.export_path_team):
            os.makedirs(self.export_path_team)

        self.get_project_templates()
        projects, stats = self.get_projects(prefer_cache=False)
        stats_queue = queue.Queue()
        for p in projects:
            LOG.info(f"extracting project {p['name']}")
            tasks, task_stats, modified_gids = self.get_project_tasks(p, prefer_cache=False)
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

        LOG.info(f"completed extraction of {len(projects)} projects.")
        while not stats_queue.empty():
            _stats = stats_queue.get()
            stats.combine(_stats)

        LOG.info(f"stats: {stats}")
        end = time.time()
        LOG.info(f"extraction to {self.export_path} completed in {round(end - start, 3)} secs.")


def main():
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
            migrate_schema,
            import_export_dir,
            rebuild_task_search_fts,
        )

        if not os.path.isdir(args.export_path):
            LOG.error(f"export path '{args.export_path}' does not exist or is not a directory")
            return

        db_path = args.db or os.path.join(args.export_path, "asana.db")
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
        for t in teams:
            LOG.info(f"exporting team '{t['name']}'")
            team_ae = AsanaExtractor(
                token=args.token,
                workspace=args.workspace,
                teamname=t["name"],
                export_path=args.export_path,
                project_include_filter=args.project_filter,
                project_exclude_filter=args.exclude_projects,
                force_update=args.force_update,
            )
            team_ae.run()
    else:
        ae.run()

    LOG.info("done.")


if __name__ == "__main__":
    main()

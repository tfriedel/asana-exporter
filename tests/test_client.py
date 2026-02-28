"""Tests for asana_exporter.client."""

import json
import queue
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from asana_exporter.client import (
    AsanaExtractor,
    AsanaProjectTaskAttachments,
    AsanaProjectTasks,
    AsanaProjectTaskStories,
    AsanaTaskSubTasks,
    ExtractorStats,
)


class TestWorkspaceProperty:
    """Tests for AsanaExtractor.workspace property."""

    def test_workspace_not_found_raises_valueerror(self, tmp_path: str) -> None:
        """Raise ValueError with a descriptive message when workspace name not found."""
        extractor = AsanaExtractor(
            token="fake-token",
            workspace="nonexistent-workspace",
            teamname=None,
            export_path=str(tmp_path),
        )
        mock_client = MagicMock()
        mock_workspaces_api = MagicMock()
        mock_workspaces_api.get_workspaces.return_value = [
            {"gid": "123", "name": "My Workspace"},
            {"gid": "456", "name": "Other Workspace"},
        ]

        with (
            patch.object(
                type(extractor),
                "client",
                new_callable=lambda: property(lambda self: mock_client),
            ),
            patch(
                "asana_exporter.client.asana.WorkspacesApi",
                return_value=mock_workspaces_api,
            ),
            pytest.raises(ValueError, match="nonexistent-workspace"),
        ):
            _ = extractor.workspace


class TestGetSubtaskAttachments:
    """Tests for AsanaExtractor.get_subtask_attachments."""

    def test_stats_queue_receives_stats_when_no_subtasks(self, tmp_path: Path) -> None:
        """stats_queue.put() must be called even when subtasks list is empty."""
        extractor = AsanaExtractor(
            token="fake-token",
            workspace="12345",
            teamname="Engineering",
            export_path=str(tmp_path),
        )
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}

        # Build directory that AsanaTaskSubTasks expects
        projects_dir = tmp_path / "projects"
        subtask_dir = projects_dir / "proj1" / "tasks" / "task1"
        subtask_dir.mkdir(parents=True)
        (subtask_dir / "subtasks.json").write_text("[]")

        stats_queue: queue.Queue[ExtractorStats] = queue.Queue()
        event = threading.Event()
        event.set()

        mock_client = MagicMock()
        with (
            patch.object(
                type(extractor),
                "client",
                new_callable=lambda: property(lambda self: mock_client),
            ),
            patch.object(
                type(extractor),
                "projects_dir",
                new_callable=lambda: property(lambda self: projects_dir),
            ),
        ):
            extractor.get_subtask_attachments(
                project, task, event, stats_queue, update_from_api=False
            )

        assert not stats_queue.empty(), "stats_queue should have received stats"
        stats = stats_queue.get()
        assert stats["num_subtask_attachments"] == 0

    def test_does_not_call_api_for_subtasks_on_cache_miss(self, tmp_path: Path) -> None:
        """get_subtask_attachments should read subtasks from cache only, never from API."""
        extractor = AsanaExtractor(
            token="fake-token",
            workspace="12345",
            teamname="Engineering",
            export_path=str(tmp_path),
        )
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}

        # Build directory but do NOT create subtasks.json — simulates cache miss
        projects_dir = tmp_path / "projects"
        subtask_dir = projects_dir / "proj1" / "tasks" / "task1"
        subtask_dir.mkdir(parents=True)

        stats_queue: queue.Queue[ExtractorStats] = queue.Queue()
        event = threading.Event()
        event.set()

        mock_client = MagicMock()
        mock_tasks_api = MagicMock()
        mock_tasks_api.get_subtasks_for_task.return_value = []

        with (
            patch.object(
                type(extractor),
                "client",
                new_callable=lambda: property(lambda self: mock_client),
            ),
            patch.object(
                type(extractor),
                "projects_dir",
                new_callable=lambda: property(lambda self: projects_dir),
            ),
            patch(
                "asana_exporter.client.asana.TasksApi",
                return_value=mock_tasks_api,
            ),
        ):
            extractor.get_subtask_attachments(project, task, event, stats_queue)

        mock_tasks_api.get_subtasks_for_task.assert_not_called()


class TestAsanaProjectTasks:
    """Tests for AsanaProjectTasks._from_api."""

    def test_incremental_sync_stores_full_task_records(self, tmp_path: Path) -> None:
        """Modified tasks should store full API records in tasks.json, not compact."""
        project = {"gid": "proj1", "name": "Project One"}
        projects_dir = tmp_path / "projects"
        root_path = projects_dir / "proj1"
        root_path.mkdir(parents=True)

        # Set up existing tasks.json and sync meta (simulate previous full sync)
        existing_task = {"gid": "task1", "name": "Old Name", "notes": "old notes"}
        (root_path / "tasks.json").write_text(json.dumps([existing_task]))
        (root_path / ".sync_meta.json").write_text(
            json.dumps({"last_sync": "2024-01-01T00:00:00", "version": 1})
        )
        (root_path / "tasks" / "task1").mkdir(parents=True)

        # Compact record returned by get_tasks (modified_since query)
        compact_task = {"gid": "task1", "name": "Updated Name"}
        # Full record returned by get_task
        full_task = {"gid": "task1", "name": "Updated Name", "notes": "new notes"}

        mock_client = MagicMock()
        mock_tasks_api = MagicMock()
        mock_tasks_api.get_tasks.return_value = [compact_task]
        mock_tasks_api.get_task.return_value = full_task

        apt = AsanaProjectTasks(mock_client, project, projects_dir)

        with patch("asana_exporter.client.asana.TasksApi", return_value=mock_tasks_api):
            result = apt._from_api(readonly=False)

        # The returned list should contain the FULL task record
        assert len(result) == 1
        assert result[0]["notes"] == "new notes"

        # Also verify what was written to tasks.json
        written = json.loads((root_path / "tasks.json").read_text())
        assert written[0]["notes"] == "new notes"


class TestReadonlyFlag:
    """Tests for readonly flag being respected in _from_api methods."""

    def test_subtasks_from_api_does_not_write_when_readonly(self, tmp_path: Path) -> None:
        """AsanaTaskSubTasks._from_api should not write subtasks.json when readonly=True."""
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}
        projects_dir = tmp_path / "projects"

        mock_client = MagicMock()
        mock_tasks_api = MagicMock()
        mock_tasks_api.get_subtasks_for_task.return_value = [
            {"gid": "sub1", "name": "Subtask 1"},
        ]
        mock_tasks_api.get_task.return_value = {"gid": "sub1", "name": "Subtask 1", "notes": ""}

        atst = AsanaTaskSubTasks(mock_client, project, projects_dir, task)

        with patch("asana_exporter.client.asana.TasksApi", return_value=mock_tasks_api):
            result = atst._from_api(readonly=True)

        assert len(result) == 1
        assert not atst._local_store.exists(), (
            "subtasks.json should not be written when readonly=True"
        )

    def test_stories_from_api_does_not_write_when_readonly(self, tmp_path: Path) -> None:
        """AsanaProjectTaskStories._from_api should not write stories.json when readonly=True."""
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}
        projects_dir = tmp_path / "projects"

        mock_client = MagicMock()
        mock_stories_api = MagicMock()
        mock_stories_api.get_stories_for_task.return_value = [
            {"gid": "story1", "text": "A comment"},
        ]

        ats = AsanaProjectTaskStories(mock_client, project, projects_dir, task)

        with patch("asana_exporter.client.asana.StoriesApi", return_value=mock_stories_api):
            result = ats._from_api(readonly=True)

        assert len(result) == 1
        assert not ats._local_store.exists(), (
            "stories.json should not be written when readonly=True"
        )

    def test_attachments_from_api_does_not_write_when_readonly(self, tmp_path: Path) -> None:
        """AsanaProjectTaskAttachments._from_api should not write when readonly=True."""
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}
        projects_dir = tmp_path / "projects"

        mock_client = MagicMock()
        mock_attachments_api = MagicMock()
        mock_attachments_api.get_attachments_for_object.return_value = [
            {"gid": "att1", "name": "file.png"},
        ]
        mock_attachments_api.get_attachment.return_value = {
            "gid": "att1",
            "name": "file.png",
            "download_url": None,
        }

        ata = AsanaProjectTaskAttachments(mock_client, project, projects_dir, task)

        with patch(
            "asana_exporter.client.asana.AttachmentsApi",
            return_value=mock_attachments_api,
        ):
            result = ata._from_api(readonly=True)

        assert len(result) == 1
        assert not ata._local_store.exists(), (
            "attachments.json should not be written when readonly=True"
        )


class TestAttachmentFullRecords:
    """Tests for AsanaProjectTaskAttachments storing full API records."""

    def test_attachments_json_contains_full_records(self, tmp_path: Path) -> None:
        """attachments.json should store full records from get_attachment, not compact."""
        project = {"gid": "proj1", "name": "Project One"}
        task = {"gid": "task1", "name": "My Task"}
        projects_dir = tmp_path / "projects"

        compact_att = {"gid": "att1", "name": "file.png"}
        full_att = {
            "gid": "att1",
            "name": "file.png",
            "download_url": None,
            "host": "asana",
            "size": 2048,
        }

        mock_client = MagicMock()
        mock_attachments_api = MagicMock()
        mock_attachments_api.get_attachments_for_object.return_value = [compact_att]
        mock_attachments_api.get_attachment.return_value = full_att

        ata = AsanaProjectTaskAttachments(mock_client, project, projects_dir, task)

        with patch(
            "asana_exporter.client.asana.AttachmentsApi",
            return_value=mock_attachments_api,
        ):
            result = ata._from_api(readonly=False)

        # Return value should be FULL records
        assert len(result) == 1
        assert result[0]["host"] == "asana"
        assert result[0]["size"] == 2048

        # attachments.json should contain full records
        written = json.loads(ata._local_store.read_text())
        assert written[0]["host"] == "asana"
        assert written[0]["size"] == 2048

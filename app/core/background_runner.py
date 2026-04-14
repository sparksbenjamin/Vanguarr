from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from app.core.services import VanguarrService


logger = logging.getLogger("vanguarr.background")


class BackgroundEngineRunner:
    def __init__(self, service: VanguarrService) -> None:
        self.service = service
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    def launch_profile_architect(self, username: str | None = None) -> tuple[bool, str]:
        return self._launch(
            engine_name="profile_architect",
            label="Profile Architect",
            target=username,
            job_factory=lambda: self.service.run_profile_architect(username),
        )

    async def launch_profile_architect_async(self, username: str | None = None) -> tuple[bool, str]:
        return self.launch_profile_architect(username)

    def launch_decision_engine(self, username: str | None = None) -> tuple[bool, str]:
        return self._launch(
            engine_name="decision_engine",
            label="Decision Engine",
            target=username,
            job_factory=lambda: self.service.run_decision_engine(username),
        )

    async def launch_decision_engine_async(self, username: str | None = None) -> tuple[bool, str]:
        return self.launch_decision_engine(username)

    def launch_suggested_for_you(self, username: str | None = None) -> tuple[bool, str]:
        return self._launch(
            engine_name="suggested_for_you",
            label="Suggested For You",
            target=username,
            job_factory=lambda: self.service.run_suggested_for_you(username),
        )

    async def launch_suggested_for_you_async(self, username: str | None = None) -> tuple[bool, str]:
        return self.launch_suggested_for_you(username)

    def launch_library_sync(self) -> tuple[bool, str]:
        return self._launch(
            engine_name="library_sync",
            label="Library Sync",
            target="the Jellyfin library",
            job_factory=self.service.run_library_sync,
        )

    async def launch_library_sync_async(self) -> tuple[bool, str]:
        return self.launch_library_sync()

    def is_running(self, engine_name: str) -> bool:
        task = self._tasks.get(engine_name)
        return bool(task and not task.done())

    async def shutdown(self) -> None:
        active_tasks = [task for task in self._tasks.values() if not task.done()]
        if not active_tasks:
            return

        for task in active_tasks:
            task.cancel()

        done, pending = await asyncio.wait(active_tasks, timeout=1.0)
        for task in done:
            with suppress(asyncio.CancelledError):
                try:
                    task.result()
                except Exception:
                    logger.exception("Background engine task surfaced an error during shutdown cleanup.")
        if pending:
            logger.warning("Background shutdown left %s engine task(s) pending.", len(pending))

    def _launch(
        self,
        *,
        engine_name: str,
        label: str,
        target: str | None,
        job_factory: Callable[[], Awaitable[dict[str, Any]]],
    ) -> tuple[bool, str]:
        existing_task = self._tasks.get(engine_name)
        if existing_task is not None and not existing_task.done():
            logger.info("%s launch skipped because a run is already in progress.", label)
            return False, f"{label} is already running."

        task = asyncio.create_task(job_factory(), name=f"vanguarr:{engine_name}")
        self._tasks[engine_name] = task
        task.add_done_callback(lambda finished_task, name=engine_name, title=label: self._handle_completion(name, title, finished_task))

        target_label = target or "all users"
        logger.info("%s queued in the background for target=%s", label, target_label)
        return True, f"{label} started in the background for {target_label}."

    def _handle_completion(self, engine_name: str, label: str, task: asyncio.Task[Any]) -> None:
        if self._tasks.get(engine_name) is task:
            self._tasks.pop(engine_name, None)

        try:
            result = task.result()
        except asyncio.CancelledError:
            logger.info("%s background task was cancelled during shutdown.", label)
            return
        except Exception:
            logger.exception("%s background task failed unexpectedly.", label)
            return

        summary = str((result or {}).get("summary") or "").strip()
        if summary:
            logger.info("%s background task finished: %s", label, summary)
        else:
            logger.info("%s background task finished.", label)

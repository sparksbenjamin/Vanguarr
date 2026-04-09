from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.background_runner import BackgroundEngineRunner
from app.core.services import VanguarrService
from app.core.settings import Settings


class EngineScheduler:
    def __init__(self, settings: Settings, service: VanguarrService, runner: BackgroundEngineRunner) -> None:
        self.settings = settings
        self.service = service
        self.runner = runner
        self._scheduler: AsyncIOScheduler | None = None

    def _current_settings(self) -> Settings:
        if hasattr(self.settings, "snapshot"):
            return self.settings.snapshot(force=True)
        return self.settings

    def start(self) -> None:
        self.refresh()

    def refresh(self) -> None:
        settings = self._current_settings()
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

        if not settings.scheduler_enabled:
            return

        timezone = ZoneInfo(settings.timezone)
        scheduler = AsyncIOScheduler(timezone=timezone)
        scheduler.add_job(
            self.runner.launch_profile_architect,
            trigger=CronTrigger.from_crontab(settings.profile_cron, timezone=timezone),
            id="profile_architect",
            name="Profile Architect",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        scheduler.add_job(
            self.runner.launch_decision_engine,
            trigger=CronTrigger.from_crontab(settings.decision_cron, timezone=timezone),
            id="decision_engine",
            name="Decision Engine",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        scheduler.start()
        self._scheduler = scheduler

    def shutdown(self) -> None:
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    def snapshot(self) -> list[dict[str, Any]]:
        if self._scheduler is None:
            return [
                {
                    "id": "scheduler",
                    "name": "Scheduler",
                    "trigger": "disabled",
                    "next_run_time": None,
                }
            ]

        jobs = self._scheduler.get_jobs()
        return [
            {
                "id": job.id,
                "name": job.name,
                "trigger": str(job.trigger),
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            }
            for job in jobs
        ]

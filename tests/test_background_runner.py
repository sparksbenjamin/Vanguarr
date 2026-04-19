import asyncio

from app.core.background_runner import BackgroundEngineRunner


class FakeService:
    def __init__(self) -> None:
        self.profile_calls: list[str | None] = []
        self.decision_calls: list[str | None] = []
        self.preview_calls: list[str | None] = []
        self.library_calls = 0
        self.request_sync_calls: list[str | None] = []
        self.profile_release = asyncio.Event()
        self.operation_events: list[dict[str, str]] = []
        self.enabled_profiles: dict[str, bool] = {}

    async def run_profile_architect(self, username: str | None, *, trigger_source: str = "manual") -> dict[str, str]:
        self.profile_calls.append(username)
        await self.profile_release.wait()
        return {"summary": "Profile Architect finished."}

    async def run_decision_engine(self, username: str | None) -> dict[str, str]:
        self.decision_calls.append(username)
        await asyncio.sleep(0)
        return {"summary": "Decision Engine finished."}

    async def run_decision_preview(self, username: str | None, *, trigger_source: str = "manual") -> dict[str, str]:
        self.preview_calls.append(username)
        await asyncio.sleep(0)
        return {"summary": "Decision Dry Run finished."}

    async def run_library_sync(self, *, trigger_source: str = "manual") -> dict[str, str]:
        self.library_calls += 1
        await asyncio.sleep(0)
        return {"summary": "Library Sync finished."}

    async def run_request_status_sync(self, username: str | None = None, *, trigger_source: str = "manual") -> dict[str, str]:
        self.request_sync_calls.append(username)
        await asyncio.sleep(0)
        return {"summary": "Request Status Sync finished."}

    def record_operation_event(self, **payload: str) -> None:
        self.operation_events.append(payload)

    def is_profile_enabled(self, username: str) -> bool:
        return self.enabled_profiles.get(username, True)


def test_background_runner_prevents_duplicate_profile_launches() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_profile_architect("admin")
        duplicate_started, duplicate_message = runner.launch_profile_architect("admin")

        assert started is True
        assert "started in the background" in message
        assert duplicate_started is False
        assert duplicate_message == "Profile Architect is already running."
        assert runner.is_running("profile_architect") is True
        assert service.operation_events[0]["decision"] == "QUEUE"
        assert service.operation_events[1]["decision"] == "SKIP"

        await asyncio.sleep(0)
        assert service.profile_calls == ["admin"]

        service.profile_release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert runner.is_running("profile_architect") is False

    asyncio.run(scenario())


def test_background_runner_cleans_up_completed_decision_runs() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_decision_engine(None)

        assert started is True
        assert message == "Decision Engine started in the background for all users."
        assert service.operation_events[0]["source"] == "manual"

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.decision_calls == [None]
        assert runner.is_running("decision_engine") is False

    asyncio.run(scenario())


def test_background_runner_skips_disabled_profile_decision_launch() -> None:
    async def scenario() -> None:
        service = FakeService()
        service.enabled_profiles["alice"] = False
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_decision_engine("alice")

        assert started is False
        assert message == "Decision Engine is disabled for alice. Re-enable the profile to place requests."
        assert service.decision_calls == []
        assert runner.is_running("decision_engine") is False
        assert service.operation_events[0]["decision"] == "SKIP"
        assert service.operation_events[0]["username"] == "alice"

    asyncio.run(scenario())


def test_background_runner_launches_library_sync() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_library_sync()

        assert started is True
        assert message == "Library Sync started in the background for the Jellyfin library."

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.library_calls == 1
        assert runner.is_running("library_sync") is False

    asyncio.run(scenario())


def test_background_runner_launches_decision_preview() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_decision_preview("alice")

        assert started is True
        assert message == "Decision Dry Run started in the background for alice."

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.preview_calls == ["alice"]
        assert runner.is_running("decision_preview") is False

    asyncio.run(scenario())


def test_background_runner_launches_request_status_sync() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, message = runner.launch_request_status_sync()

        assert started is True
        assert message == "Request Status Sync started in the background for all users."

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.request_sync_calls == [None]
        assert runner.is_running("request_status_sync") is False

    asyncio.run(scenario())


def test_background_runner_async_wrappers_launch_jobs_on_event_loop() -> None:
    async def scenario() -> None:
        service = FakeService()
        runner = BackgroundEngineRunner(service)

        started, library_message = await runner.launch_library_sync_async()
        decision_started, decision_message = await runner.launch_decision_engine_async(None)
        preview_started, preview_message = await runner.launch_decision_preview_async("alice")
        sync_started, sync_message = await runner.launch_request_status_sync_async()

        assert started is True
        assert library_message == "Library Sync started in the background for the Jellyfin library."
        assert decision_started is True
        assert decision_message == "Decision Engine started in the background for all users."
        assert preview_started is True
        assert preview_message == "Decision Dry Run started in the background for alice."
        assert sync_started is True
        assert sync_message == "Request Status Sync started in the background for all users."
        assert service.operation_events[0]["source"] == "scheduler"
        assert service.operation_events[1]["source"] == "scheduler"
        assert service.operation_events[2]["source"] == "scheduler"
        assert service.operation_events[3]["source"] == "scheduler"

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.library_calls == 1
        assert service.decision_calls == [None]
        assert service.preview_calls == ["alice"]
        assert service.request_sync_calls == [None]

    asyncio.run(scenario())

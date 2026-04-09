import asyncio

from app.core.background_runner import BackgroundEngineRunner


class FakeService:
    def __init__(self) -> None:
        self.profile_calls: list[str | None] = []
        self.decision_calls: list[str | None] = []
        self.library_calls = 0
        self.profile_release = asyncio.Event()

    async def run_profile_architect(self, username: str | None) -> dict[str, str]:
        self.profile_calls.append(username)
        await self.profile_release.wait()
        return {"summary": "Profile Architect finished."}

    async def run_decision_engine(self, username: str | None) -> dict[str, str]:
        self.decision_calls.append(username)
        await asyncio.sleep(0)
        return {"summary": "Decision Engine finished."}

    async def run_library_sync(self) -> dict[str, str]:
        self.library_calls += 1
        await asyncio.sleep(0)
        return {"summary": "Library Sync finished."}


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

        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert service.decision_calls == [None]
        assert runner.is_running("decision_engine") is False

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

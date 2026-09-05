import pytest

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def short_notification_grace(monkeypatch):
    """Real grace is 10 s; tests would otherwise wait it out twice per unverified command."""
    import smartrent.device as device_mod

    monkeypatch.setattr(device_mod, "NOTIFICATION_GRACE", 0.08)

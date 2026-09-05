"""
Command confirmation semantics of ``Device.async_set_attribute`` via DoorLock.

No network: the client is a stub whose ``_async_send_command`` records sends
and, per test, arranges for the hub's report to arrive (or not) through the
same ``_update`` path the websocket updater uses.
"""
import asyncio

import pytest

from smartrent.lock import DoorLock
from smartrent.utils import CommandFailedError


class StubClient:
    def __init__(self):
        self.sends = []
        self.reachable = True

    def _subscribe_device_to_updater(self, device):
        pass

    def _unsubscribe_device_to_updater(self, device):
        pass

    async def _async_send_command(self, device, attribute_name, value):
        self.sends.append((attribute_name, value))


def make_lock(locked):
    client = StubClient()
    lock = DoorLock(1, client)
    lock._name = "Front Door"
    lock._locked = locked
    return lock, client


def report(lock, value, delay):
    async def _later():
        await asyncio.sleep(delay)
        await lock._update({"type": "DoorLock", "name": "locked", "last_read_state": value})

    return asyncio.create_task(_later())


@pytest.mark.asyncio
async def test_confirmed_by_hub_report():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.05)
    await lock.async_set_locked(False)
    result = lock.get_last_command()
    assert client.sends == [("locked", "false")]
    assert result.outcome == "confirmed" and result.attempts == 1
    assert result.latency is not None and 0.04 <= result.latency < 1
    assert lock.get_locked() is False
    assert lock.get_pending_locked() is None


@pytest.mark.asyncio
async def test_no_report_retries_then_fails_without_faking_state():
    lock, client = make_lock(locked=True)
    with pytest.raises(CommandFailedError) as excinfo:
        await lock.async_set_attribute("locked", "false", confirm_timeout=0.05, retries=1)
    assert client.sends == [("locked", "false")] * 2
    result = lock.get_last_command()
    assert result.outcome == "failed" and result.attempts == 2
    assert "never reported locked=false" in str(excinfo.value)
    # the cache still says what the hub last told us
    assert lock.get_locked() is True
    assert lock.get_pending_locked() is None


@pytest.mark.asyncio
async def test_report_after_retry_counts_as_confirmed():
    lock, client = make_lock(locked=False)
    report(lock, "true", 0.12)  # lands during the second attempt's wait
    result = await lock.async_set_attribute("locked", "true", confirm_timeout=0.08, retries=1)
    assert result.outcome == "confirmed" and result.attempts == 2
    assert len(client.sends) == 2
    assert lock.get_locked() is True


@pytest.mark.asyncio
async def test_already_in_requested_state_is_sent_once_and_not_awaited():
    lock, client = make_lock(locked=True)
    result = await lock.async_set_attribute("locked", "true", confirm_timeout=5)
    assert result.outcome == "unchanged" and result.attempts == 1
    assert client.sends == [("locked", "true")]
    assert result.latency is None


@pytest.mark.asyncio
async def test_pending_state_is_visible_while_in_flight_and_callbacks_fire():
    lock, client = make_lock(locked=True)
    seen = []
    lock.set_update_callback(lambda: seen.append(lock.get_pending_locked()))
    report(lock, "false", 0.05)
    await lock.async_set_locked(False)
    # callbacks: start (pending False), the hub report (pending still set), end (None)
    assert seen[0] is False
    assert seen[-1] is None


@pytest.mark.asyncio
async def test_same_command_in_flight_rides_along():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.1)
    first = asyncio.create_task(lock.async_set_locked(False))
    await asyncio.sleep(0.01)
    await lock.async_set_locked(False)  # joins the in-flight command
    await first
    assert client.sends == [("locked", "false")]
    assert lock.get_last_command().outcome == "confirmed"


@pytest.mark.asyncio
async def test_poll_can_confirm_a_missed_event():
    lock, client = make_lock(locked=True)

    async def poll_later():
        await asyncio.sleep(0.05)
        lock._fetch_state_helper(
            {"name": "Front Door", "attributes": [{"name": "locked", "state": "false"}, {"name": "notifications", "state": "x"}]}
        )
        lock._resolve_confirmation("locked", "false")

    asyncio.create_task(poll_later())
    result = await lock.async_set_attribute("locked", "false", confirm_timeout=1)
    assert result.outcome == "confirmed"


@pytest.mark.asyncio
async def test_server_rejection_surfaces_as_failed():
    lock, client = make_lock(locked=True)

    async def reject(device, attribute_name, value):
        raise CommandFailedError("SmartRent rejected command: nope")

    client._async_send_command = reject
    with pytest.raises(CommandFailedError):
        await lock.async_set_locked(False)
    assert lock.get_last_command().outcome == "failed"
    assert lock.get_pending_locked() is None

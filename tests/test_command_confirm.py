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
        await lock.async_set_attribute("locked", "false", retry_after=(0.03, 0.03, 0.03), deadline=0.2)
    # sends at 0, 0.03, 0.06, 0.09 then the final wait runs out the deadline
    assert client.sends == [("locked", "false")] * 4
    result = lock.get_last_command()
    assert result.outcome == "failed" and result.attempts == 4
    assert "never reported locked=false" in str(excinfo.value)
    # the cache still says what the hub last told us
    assert lock.get_locked() is True
    assert lock.get_pending_locked() is None


@pytest.mark.asyncio
async def test_report_after_retry_counts_as_confirmed():
    lock, client = make_lock(locked=False)
    report(lock, "true", 0.12)  # lands during the second attempt's wait
    result = await lock.async_set_attribute("locked", "true", retry_after=(0.08, 0.5), deadline=2)
    assert result.outcome == "confirmed" and result.attempts == 2
    assert len(client.sends) == 2
    assert lock.get_locked() is True


@pytest.mark.asyncio
async def test_already_in_requested_state_is_sent_once_and_not_awaited():
    lock, client = make_lock(locked=True)
    result = await lock.async_set_attribute("locked", "true", deadline=5)
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
    result = await lock.async_set_attribute("locked", "false", retry_after=(0.5,), deadline=1)
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


@pytest.mark.asyncio
async def test_deadline_caps_the_schedule_and_a_late_report_still_wins():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.14)  # after the third send, before the deadline
    result = await lock.async_set_attribute("locked", "false", retry_after=(0.05, 0.05, 5.0), deadline=0.3)
    assert result.outcome == "confirmed" and result.attempts == 3
    assert len(client.sends) == 3

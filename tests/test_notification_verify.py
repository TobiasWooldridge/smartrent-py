"""A hub report must be backed by the lock's own operation notification."""
import asyncio

import pytest

from smartrent.lock import DoorLock
from tests.test_command_confirm import StubClient, make_lock, report


def notify(lock, notification, delay):
    async def _later():
        await asyncio.sleep(delay)
        await lock._update({"type": "DoorLock", "name": "notifications", "last_read_state": notification})

    return asyncio.create_task(_later())


@pytest.mark.asyncio
async def test_report_plus_notification_is_verified():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.02, notification=False)
    notify(lock, "UNLOCK_VIA_RF", 0.05)
    result = await lock.async_set_attribute("locked", "false", retry_after=(1,), deadline=2)
    assert result.outcome == "confirmed" and result.verified is True and result.attempts == 1
    assert client.sends == [("locked", "false")]


@pytest.mark.asyncio
async def test_phantom_report_triggers_a_verification_resend():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.02, notification=False)  # hub says unlocked...
    notify(lock, "UNLOCK_VIA_RF", 0.15)  # ...the bolt only moves after the re-send
    result = await lock.async_set_attribute("locked", "false", retry_after=(1,), deadline=2)
    assert result.outcome == "confirmed" and result.attempts == 2 and result.verified is True
    assert client.sends == [("locked", "false")] * 2


@pytest.mark.asyncio
async def test_never_notified_is_confirmed_but_unverified():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.02, notification=False)
    result = await lock.async_set_attribute("locked", "false", retry_after=(1,), deadline=2)
    assert result.outcome == "confirmed" and result.verified is False and result.attempts == 2
    assert lock.get_locked() is False  # hub's word stands, flagged as unproven


@pytest.mark.asyncio
async def test_notification_alone_confirms_when_the_report_is_lost():
    lock, client = make_lock(locked=True)
    notify(lock, "UNLOCK_VIA_RF", 0.03)
    result = await lock.async_set_attribute("locked", "false", retry_after=(1,), deadline=2)
    assert result.outcome == "confirmed" and result.verified is True
    assert lock.get_locked() is False


@pytest.mark.asyncio
async def test_lock_command_accepts_the_raw_alarm_type_24():
    lock, client = make_lock(locked=False)
    report(lock, "true", 0.02, notification=False)
    notify(lock, "ALARM_TYPE_24_LEVEL_1", 0.04)
    result = await lock.async_set_attribute("locked", "true", retry_after=(1,), deadline=2)
    assert result.verified is True and result.attempts == 1


@pytest.mark.asyncio
async def test_wrong_notification_does_not_verify():
    lock, client = make_lock(locked=True)
    report(lock, "false", 0.02, notification=False)
    notify(lock, "KEY_OR_THUMBTURN_UNLOCK", 0.04)
    result = await lock.async_set_attribute("locked", "false", retry_after=(1,), deadline=2)
    assert result.verified is False

"""``Client._async_send_command`` prefers the live update websocket."""
import asyncio

import pytest

from smartrent.utils import Client


class FakeWs:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def send(self, payload):
        if self.fail:
            raise ConnectionError("closed")
        self.sent.append(payload)


class FakeDevice:
    _device_id = 42
    _name = "Front Door"


def make_client():
    client = Client.__new__(Client)  # no session / login
    client._ws = None
    return client


@pytest.mark.asyncio
async def test_live_socket_used_when_available():
    client = make_client()
    client._ws = FakeWs()
    fresh = []

    async def fresh_send(device, payload):
        fresh.append(payload)

    client._async_send_payload = fresh_send
    await client._async_send_command(FakeDevice(), "locked", "false", prefer_live=True)
    assert len(client._ws.sent) == 1 and '"locked", "value": "false"' in client._ws.sent[0]
    assert fresh == []


@pytest.mark.asyncio
async def test_fresh_connection_when_no_live_socket_or_send_fails():
    for ws in (None, FakeWs(fail=True)):
        client = make_client()
        client._ws = ws
        fresh = []

        async def fresh_send(device, payload):
            fresh.append(payload)

        client._async_send_payload = fresh_send
        await client._async_send_command(FakeDevice(), "locked", "true", prefer_live=True)
        assert len(fresh) == 1


@pytest.mark.asyncio
async def test_default_is_a_fresh_connection():
    client = make_client()
    client._ws = FakeWs()
    fresh = []

    async def fresh_send(device, payload):
        fresh.append(payload)

    client._async_send_payload = fresh_send
    await client._async_send_command(FakeDevice(), "locked", "true")
    assert client._ws.sent == [] and len(fresh) == 1

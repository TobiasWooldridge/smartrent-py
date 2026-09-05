import asyncio
import inspect
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from .utils import COMMAND_DEADLINE, COMMAND_RETRY_AFTER, Client, CommandFailedError

_LOGGER = logging.getLogger(__name__)


@dataclass
class CommandResult:
    """
    Outcome of one ``Device.async_set_attribute`` call.

    ``outcome`` is ``pending`` while in flight, then ``confirmed`` (the hub
    reported the new value; ``latency`` is seconds from first send to that
    report), ``unchanged`` (the device already reported the requested value,
    so no report was expected) or ``failed`` (no report after every attempt;
    ``error`` says why). ``attempts`` counts sends.
    """

    attribute: str
    value: str
    started: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    attempts: int = 0
    outcome: str = "pending"
    latency: Optional[float] = None
    error: Optional[str] = None


class Device:
    """
    Base class for SmartRent devices
    """

    def __init__(self, device_id: Union[str, int], client: Client):
        self._device_id = int(device_id)
        self._name: str = ""
        self._online: Optional[bool] = None
        self._battery_powered: Optional[bool] = None
        self._battery_level: Optional[int] = None
        self._update_callback_funcs: List[Callable[[None], None]] = []

        self._client: Client = client

        # (attribute, value, future) while a command awaits the hub's report
        self._pending_confirmation: Optional[
            Tuple[str, str, "asyncio.Future[bool]"]
        ] = None
        self._last_command: Optional[CommandResult] = None

    def __del__(self):
        self.stop_updater()

    def get_name(self) -> Optional[str]:
        """
        Gets the name of the device, if known
        """
        return self._name

    def get_online(self) -> Optional[bool]:
        """
        Gets if device is online or not
        """
        return self._online

    def get_battery_powered(self) -> Optional[bool]:
        """
        Gets if devices is battery powered
        """
        return self._battery_powered

    def get_reachable(self) -> bool:
        """
        True while the shared client's REST polling of SmartRent succeeds
        """
        return self._client.reachable

    def get_battery_level(self) -> Optional[int]:
        """
        Gets devices battery level (assuming device is battery powered)
        """
        return self._battery_level

    def get_last_command(self) -> Optional[CommandResult]:
        """
        Result of the most recent ``async_set_attribute`` call, or None
        """
        return self._last_command

    def get_pending_command(self) -> Optional[Tuple[str, str]]:
        """
        ``(attribute, value)`` of the command awaiting the hub, or None
        """
        pending = self._pending_confirmation
        return (pending[0], pending[1]) if pending else None

    def _get_attribute(self, attribute: str) -> Optional[str]:
        """
        Current cached value of ``attribute`` as SmartRent spells it
        (e.g. ``"true"``), or None if unknown. Subclasses override so
        ``async_set_attribute`` can tell a no-op from a lost command.
        """
        return None

    def _resolve_confirmation(self, attribute: Optional[str], state: Any):
        """
        Completes the pending command's future when a report of
        ``attribute`` at the requested value arrives (websocket or poll)
        """
        pending = self._pending_confirmation
        if (
            pending
            and pending[0] == attribute
            and pending[1] == str(state)
            and not pending[2].done()
        ):
            pending[2].set_result(True)

    async def async_set_attribute(
        self,
        attribute: str,
        value: str,
        *,
        retry_after: Sequence[float] = COMMAND_RETRY_AFTER,
        deadline: float = COMMAND_DEADLINE,
    ) -> CommandResult:
        """
        Sends ``attribute=value`` and waits for the hub to report it.

        The server accepting the command (its phx_reply) proves nothing
        about the device: on a slow or wedged hub the report can take tens
        of seconds or never come. Commands are absolute state sets, so this
        re-sends the same payload after each silence in ``retry_after``
        (seconds since the previous send) and raises ``CommandFailedError``
        if the device has not reported the value by ``deadline`` seconds
        after the first send. A device that already reports the value gets
        the command sent once (the cache may be stale) and returns
        ``outcome="unchanged"`` without waiting. Callbacks fire when the
        command starts and ends so entities can render the in-flight state.
        """
        pending = self._pending_confirmation
        if pending and pending[0] == attribute and pending[1] == value:
            # Same command already in flight: ride along instead of racing it
            _LOGGER.info(
                "%s: %s=%s already in flight, waiting on it",
                self._name, attribute, value,
            )
            try:
                await asyncio.wait_for(asyncio.shield(pending[2]), deadline)
            except asyncio.TimeoutError:
                pass
            return self._last_command  # type: ignore[return-value]

        result = CommandResult(attribute=attribute, value=value)
        self._last_command = result
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[bool]" = loop.create_future()
        self._pending_confirmation = (attribute, value, future)
        await self._async_call_callbacks()
        started = time.monotonic()
        deadline_at = started + deadline
        gaps = list(retry_after)
        try:
            if self._get_attribute(attribute) == value:
                result.attempts = 1
                await self._client._async_send_command(
                    self, attribute_name=attribute, value=value
                )
                result.outcome = "unchanged"
                _LOGGER.info(
                    "%s: %s=%s sent; device already reports that value, "
                    "not waiting for a report",
                    self._name, attribute, value,
                )
                return result

            while True:
                result.attempts += 1
                send_started = time.monotonic()
                # First attempt rides the live socket (fast); a re-send opens
                # a fresh connection so a wedged socket cannot eat every try
                await self._client._async_send_command(
                    self,
                    attribute_name=attribute,
                    value=value,
                    prefer_live=result.attempts == 1,
                )
                remaining = deadline_at - time.monotonic()
                wait = gaps.pop(0) if gaps else remaining
                wait = max(0.0, min(wait, remaining))
                _LOGGER.info(
                    "%s: %s=%s attempt %d accepted by server in %.2fs; "
                    "hub has %.0fs before the next send (deadline in %.0fs)",
                    self._name, attribute, value, result.attempts,
                    time.monotonic() - send_started, wait, remaining,
                )
                try:
                    await asyncio.wait_for(asyncio.shield(future), wait)
                except asyncio.TimeoutError:
                    if time.monotonic() >= deadline_at:
                        break
                    _LOGGER.warning(
                        "%s: no hub report of %s=%s %.0fs after attempt %d, re-sending",
                        self._name, attribute, value,
                        time.monotonic() - send_started, result.attempts,
                    )
                    continue
                result.outcome = "confirmed"
                result.latency = round(time.monotonic() - started, 2)
                _LOGGER.info(
                    "%s: %s=%s confirmed by hub %.1fs after first send (%d attempt%s)",
                    self._name, attribute, value, result.latency,
                    result.attempts, "" if result.attempts == 1 else "s",
                )
                return result

            result.outcome = "failed"
            result.error = (
                f"hub never reported {attribute}={value}: {result.attempts} "
                f"sends over {time.monotonic() - started:.0f}s"
            )
            _LOGGER.error("%s: %s", self._name, result.error)
            raise CommandFailedError(f"{self._name}: {result.error}")
        except CommandFailedError as exc:
            if result.outcome == "pending":
                result.outcome = "failed"
                result.error = str(exc)
            raise
        finally:
            self._pending_confirmation = None
            if not future.done():
                future.cancel()
            await self._async_call_callbacks()

    @staticmethod
    def _structure_attrs(attrs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Converts device json object to hirearchical list of attributes

        ``attrs``: List of device attributes
        """
        structure: Dict[str, Any] = {}

        for attr in attrs:
            name: str = str(attr.get("name"))
            state: str = str(attr.get("state"))

            structure[name] = state

        _LOGGER.info("constructed attribute structure: %s", structure)
        return structure

    def _fetch_state_helper(self, data: Dict[str, str]):
        """
        Called by ``_async_fetch_state``

        Converts event dict to device param info
        """
        raise NotImplementedError

    async def _async_fetch_state(self):
        """
        Fetches device information from SmartRent api

        Calls ``_fetch_state_helper`` so device can parse out
        info and update it's state.

        Calls function passed into ``set_update_callback`` if it exists.
        """
        _LOGGER.info("%s: Fetching data from device endpoint...", self._name)
        device_data = await self._client.async_get_device_data(self._device_id)

        device_at_start = {
            k: v for k, v in vars(self).items() if k != "_pending_confirmation"
        }

        self._battery_level = device_data.get("battery_level")
        self._battery_powered = device_data.get("battery_powered")
        self._online = device_data.get("online")

        self._fetch_state_helper(device_data)

        # A poll can be the first sight of a commanded value (missed event)
        pending = self._pending_confirmation
        if pending:
            attrs = self._structure_attrs(device_data.get("attributes", []))
            self._resolve_confirmation(pending[0], attrs.get(pending[0]))

        device_at_end = {
            k: v for k, v in vars(self).items() if k != "_pending_confirmation"
        }

        # If device attrs updated, call callbacks
        if not device_at_start == device_at_end:
            await self._async_call_callbacks()

    def start_updater(self):
        """
        Allows device to update it's attrs in the background
        """
        self._client._subscribe_device_to_updater(self)

    def stop_updater(self):
        """
        Turns off automatic attr updates
        """
        self._client._unsubscribe_device_to_updater(self)

    def set_update_callback(self, func) -> None:
        """
        Allows callback to be fired when ``Client._async_update_state``
        or ``_async_fetch_state`` gets new information
        """

        self._update_callback_funcs.append(func)

    def unset_update_callback(self, func) -> None:
        """
        Removes callback from being fired when ``Client._async_update_state``
        or ``_async_fetch_state`` gets new information
        """
        try:
            self._update_callback_funcs.remove(func)
        except ValueError:
            pass

    def _update_parser(self, event: Dict[str, Any]) -> None:
        """
        Called by ``Client._async_update_state``

        Converts event dict to device attr info
        """
        raise NotImplementedError

    async def _update(self, event: Dict[str, Any]):
        """
        Recieves event dict, calls ``_update_parser`` for each device and callbacks
        """
        # handle updating of device attrs
        self._update_parser(event)

        # a report of the commanded value completes the pending command
        self._resolve_confirmation(event.get("name"), event.get("last_read_state"))

        # handle calling callbacks
        await self._async_call_callbacks()

    async def _async_call_callbacks(self):
        """
        Handles calling all callbacks
        """
        for func in self._update_callback_funcs:
            if inspect.iscoroutinefunction(func):
                await func()
            else:
                func()

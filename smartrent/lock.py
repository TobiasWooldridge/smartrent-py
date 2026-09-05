import logging
from typing import Optional

from .device import Device
from .utils import Client

_LOGGER = logging.getLogger(__name__)


class DoorLock(Device):
    """
    Represents Lock SmartRent device
    """

    def __init__(self, device_id: int, client: Client):
        super().__init__(device_id, client)
        self._locked: Optional[bool] = None
        self._notification: Optional[str] = None

    def get_notification(self) -> Optional[str]:
        """
        Notification message for lock
        """
        return self._notification

    def get_locked(self) -> Optional[bool]:
        """
        Gets state from lock
        """
        return self._locked

    def get_pending_locked(self) -> Optional[bool]:
        """
        The locked value a command is waiting for the hub to confirm,
        or None when nothing is in flight
        """
        pending = self.get_pending_command()
        if pending and pending[0] == "locked":
            return pending[1] == "true"
        return None

    def _get_attribute(self, attribute: str) -> Optional[str]:
        if attribute == "locked" and self._locked is not None:
            return str(self._locked).lower()
        if attribute == "notifications":
            return self._notification
        return None

    async def async_set_locked(self, value: bool):
        """
        Locks or unlocks, returning once the hub reports the new state.

        Raises ``CommandFailedError`` if the hub never does; the cached
        state is only ever set from the hub's own reports, never
        optimistically, so a lost command cannot look like a success.
        """
        # Convert to lowercase just like SmartRent website does
        await self.async_set_attribute("locked", str(value).lower())

    def _fetch_state_helper(self, data: dict):
        """
        Called when ``_async_fetch_state`` returns info

        ``data`` is dict of info passed in by ``_async_fetch_state``
        """
        self._name = data["name"]

        attrs = self._structure_attrs(data["attributes"])

        self._locked = bool(attrs["locked"] == "true")
        self._notification = attrs["notifications"]

    def _update_parser(self, event: dict):
        """
        Called when ``Client._async_update_state`` returns info

        ``event`` dict passed in from ``Client._async_update_state``
        """
        _LOGGER.info("Updating DoorLock")

        if event.get("name") == "locked":
            self._locked = bool(event["last_read_state"] == "true")

        elif event.get("name") == "notifications":
            self._notification = event.get("last_read_state")

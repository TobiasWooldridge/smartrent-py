# Changelog (thw fork)

## 0.7.0 - 2026-09-05
- `Device.async_set_attribute`: a command completes only when the hub reports
  the attribute at its new value (websocket event or poll), waiting
  `COMMAND_CONFIRM_TIMEOUT` (12 s), re-sending once, then raising
  `CommandFailedError`. A device already reporting the value is sent once and
  returns `outcome="unchanged"` without waiting. `CommandResult` records
  attempts, latency and outcome; `get_pending_command()` exposes the in-flight
  command; callbacks fire at start and end.
- `DoorLock.async_set_locked` uses it and no longer sets state optimistically.
- Unit tests (`tests/`) with a stub client, no network.

## 0.6.1 - 2026-09-04
- Token refreshed proactively at poll time (`POLL_REFRESH_MARGIN`, 15 min) so a
  command never has to refresh and re-join first.

## 0.6.0 - 2026-08-29 (thw-fixes)
- Command sends read the server's phx_reply so rejections surface.
- Reachability tracking from REST polling; forced token refresh on server-side
  session kill; websocket reconnect backfill and backoff; string unique ids;
  TLS context built once off the event loop.

# Changelog (thw fork)

## 0.7.4 - 2026-09-06
- Fix: `Device.async_set_attribute` still pushed its first attempt over the
  live socket (an explicit `prefer_live=True` survived 0.7.3). It now always
  uses a fresh authenticated connection. Pushes on the live socket were seen
  to work right after the socket (re)connects and to be silently dropped
  later, which matches a token-age cutoff on the server; not worth the gamble.

## 0.7.3 - 2026-09-05
- **Live-socket sends off by default** (`prefer_live=False`): measured, the
  server never acts on or replies to `update_attributes` pushed on the
  long-lived update socket, so 0.7.2's first attempt was a silent no-op that
  cost a 5 s retry wait on every command. A fresh authenticated connection is
  the path that works.
- **Notification-verified locks.** A hub can report `locked=false` without the
  bolt moving (seen 2026-09-05 16:10; the resident needed her key). A real RF
  operation also emits `UNLOCK_VIA_RF` / `ALARM_TYPE_24_LEVEL_1` a few seconds
  later, so after the attribute report the command waits `NOTIFICATION_GRACE`
  (10 s) for it, re-sends once if it never comes, and records
  `CommandResult.verified`. A notification alone also confirms a command whose
  attribute report was lost (seen 2026-09-04 13:11). The entity shows the new
  state at the attribute report; verification only decides whether to re-send.

## 0.7.2 - 2026-09-05
- First send of a command goes over the already-open update websocket
  (authenticated, joined to every device topic), skipping the per-command TLS
  handshake and channel join (~0.5-0.9 s). Falls back to a fresh connection
  when the live socket is down or the send fails; re-sends always use a fresh
  connection. `phx_reply` errors on the live socket are logged as warnings.

## 0.7.1 - 2026-09-05
- Re-send schedule instead of a single 12 s timeout: commands are absolute
  state sets, so the same payload is re-sent after 5 s of hub silence, again at
  10 s and 17 s, and the command fails at the 25 s deadline
  (`COMMAND_RETRY_AFTER`, `COMMAND_DEADLINE`). A healthy hub reports in 2-4 s,
  so the first re-send only ever fires on a hub that is already misbehaving.

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

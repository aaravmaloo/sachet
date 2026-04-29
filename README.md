# Sachet

Sachet is a WhatsApp moderation/stats bot built in Go on top of `whatsmeow`.

## Features implemented

- `?scan`
  - Enables constant scan mode for the current group.
  - Resets and rebuilds group stats from available history backfill + live messages.
  - Uses CPU-parallel workers to process history sync batches.
- `?list`
  - Lists members in the current group (including admin roles).
- `?stats <member>`
  - Shows per-member profile stats for the current group.
  - If `<member>` is omitted, it shows the sender's stats.
- `?leaderboard`
  - Shows top message senders in the current group.

## Stored stats

For each indexed group, Sachet stores:

- Group metadata
- Group rules (announce/locked/join approval/disappearing timer)
- Total message count
- Messages per day
- Member profiles (total messages, per-day counts, first/last seen)
- Leaderboard-ready member totals

Data is persisted in JSON (`sachet-stats.json` by default). WhatsApp session state is persisted in SQLite (`wa-session.db` by default).

## Run

1. Install Go (modern version with module support).
2. Ensure SQLite build support is available for `github.com/mattn/go-sqlite3` (on Windows, that usually means a C toolchain like `gcc`/MinGW).
3. Build and run:

```bash
go build ./...
go run .
```

4. On first run, scan the QR shown in terminal from WhatsApp linked devices.

## Config (env vars)

- `SACHET_COMMAND_PREFIX` (default: `?`)
- `SACHET_WA_DB` (default: `wa-session.db`)
- `SACHET_STATS_FILE` (default: `sachet-stats.json`)
- `SACHET_HISTORY_BATCH` (default: `50`)
- `SACHET_HISTORY_MAX_REQUESTS` (default: `40`)

## Notes on `?scan`

- WhatsApp history backfill availability depends on what the primary device provides.
- Sachet requests on-demand history in chunks and continues until:
  - history appears complete, or
  - the configured max request count is reached.
- If max requests are reached, run `?scan` again to continue deeper.


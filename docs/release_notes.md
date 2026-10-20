## v1.0.0

- **Breaking:** requires Python ≥3.12 (previously ≥3.10).
- **Breaking:** core install is now minimal — BeautifulSoup, Selenium, Flask/Flask-CORS and PyYAML moved behind the new `soup`, `browser`, `relay` and `legacy` extras; the `remote` extra and its zimmauth dependency are gone.
- **Breaking:** remote push/pull now sync over `rsync` and dirark's object-store protocol instead of fabric/zimmauth; set `ASWAN_REMOTE` to an rsync-reachable path instead of configuring `ASWAN_AUTH_HEX`/`ASWAN_AUTH_PASS`.
- **Breaking:** depot storage format changed (content-addressed object store via dirark's BlobStore, flat `status.sqlite.zip` + `status-runs.json` instead of the status DAG); convert existing depots with the new `aswan migrate <legacy> <target>` command.
- **Breaking:** the CLI dashboard is now invoked as `aswan monitor <project>` instead of `aswan <project>`.
- **Breaking:** `WebExtHandler` is renamed to `RelayHandler` (`[webext]` extra → `[relay]`, `WE_*` constants → `RELAY_*`), with no backward-compatible alias.
- **Breaking:** `Project.run`/`continue_run` dropped the `force_sync` parameter and `Project()` dropped `distributed_api`; added `stall_timeout`/`task_timeout` to bound hung runs.
- **Breaking:** re-registering a URL the depot has already collected is now a no-op even if it previously failed; pass `overwrite=True` to force a refetch.
- **Breaking:** `aswan.utils.get_url_root` removed.
- Added `aswan migrate` to convert pre-1.0 depots into the current format: resumable, merges several legacy copies into one target, and reports any run it couldn't bring over.
- Fixed browser sessions leaking chromedriver/chrome processes by calling `driver.quit()` instead of `.close()`.
- Fixed status replay so a URL's collection always wins over its own registration in the same batch, and a session-broken result always loses a same-second tie to its retry.
- Fixed stalls under heavy parallel load: workers no longer write the current-run SQLite directly, avoiding contention when many workers finish at once.

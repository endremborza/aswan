# aswan

[![pypi](https://img.shields.io/pypi/v/aswan.svg)](https://pypi.org/project/aswan/)
[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.7477587.svg)](https://doi.org/10.5281/zenodo.7477587)

Parallel web data collection with structured, versioned storage. Named after the [Aswan Dam](https://en.wikipedia.org/wiki/Aswan_Dam).

Collect data from the internet quickly and in parallel, store it compressed and content-addressed, and replay or parse it later — without ever losing a run.

## Install

```bash
uv add aswan

# HTML parsing (the Soup handlers) and the monitoring dashboard
uv add "aswan[soup,monitor]"
```

Optional extras: `soup` (BeautifulSoup, needed by every `*SoupHandler`), `browser` (Selenium), `relay` (external-fetch bridge), `monitor` (Dash dashboard), `legacy` (PyYAML, for `aswan migrate`), `test`. Remote sync uses the system `rsync` binary and needs no extra.

## Quick Start

Needs the `soup` extra: without BeautifulSoup every URL of a Soup handler ends in `Statuses.PARSING_ERROR`.

```python
from aswan import Project, RequestSoupHandler

class ArticleHandler(RequestSoupHandler):
    url_root = "https://news.ycombinator.com"

    def parse(self, soup):
        for link in soup.select("a.titlelink"):
            self.register_links_to_handler([link["href"]])
        return {"title": soup.title.text}

project = Project("hn-scrape")
project.run(urls_to_overwrite={ArticleHandler: ["https://news.ycombinator.com"]})
project.commit_current_run()

# Read collected data
for event in project.depot.get_handler_events(ArticleHandler):
    print(event.url, event.content)
```

## Concepts

### Handlers

Handlers define how to fetch and parse a URL. Subclass one:

| Handler | Use case |
| --- | --- |
| `RequestSoupHandler` | HTTP + BeautifulSoup HTML parsing |
| `RequestJsonHandler` | HTTP + JSON parsing |
| `RequestHandler` | HTTP, raw bytes |
| `BrowserSoupHandler` | Chrome via Selenium + HTML parsing |
| `BrowserJsonHandler` | Chrome via Selenium + JSON |
| `BrowserHandler` | Chrome via Selenium, raw page source |

Browser handlers open a visible Chrome window unless the handler sets `headless = True`; a visible browser occupies one of the project's `max_displays`, so set `headless = True` on a machine without a display. `eager = True` switches Chrome to its eager page-load strategy. A proxy that needs authentication only works with a visible browser.

Override `parse(self, content)` to extract and return data. Return anything JSON-serializable, or raw `bytes`/`str`.

Call `self.register_links_to_handler(urls)` inside `parse()` to enqueue new URLs for collection.

```python
class ProductHandler(RequestSoupHandler):
    url_root = "https://shop.example.com"
    max_in_parallel = 4  # limit concurrency for this handler

    def parse(self, soup):
        # enqueue pagination links
        if next_page := soup.select_one("a.next"):
            self.register_links_to_handler([next_page["href"]])

        return {
            "name": soup.select_one("h1").text,
            "price": soup.select_one(".price").text,
        }
```

### Project

`Project` orchestrates collection runs and manages the data depot.

```python
project = Project(
    name="my-project",    # depot name; determines storage directory
    local_root=None,      # depot storage root; else $ASWAN_DEPOT_ROOT, else DEFAULT_DEPOT_ROOT
    max_cpu_use=4.0,      # number of CPU cores to use
    max_displays=1,       # visible (non-headless) browsers allowed at once
    stall_timeout=None,   # abort the run if the scheduler makes no progress for this many seconds
    task_timeout=None,    # fail a single URL whose handling takes longer than this
    debug=False,
)
```

`DEFAULT_DEPOT_ROOT` and the other defaults live in `aswan.constants`.

**Running a collection:**

```python
# Add URLs only if not already in the depot
project.run(urls_to_register={ProductHandler: ["https://shop.example.com/category/1"]})

# Force-add URLs, overwriting prior status
project.run(urls_to_overwrite={ProductHandler: ["https://shop.example.com/sale"]})

# Test run: collects each handler's test_urls; it can't be committed and the next run() discards it
project.run(test_run=True)

# Commit and archive the completed run
project.commit_current_run()
```

**Registration & deduplication:**

A URL already in the depot's history is **not** fetched again when registered again: `urls_to_register` (and `register_links_to_handler`) only enqueue URLs the depot has never seen. That holds for failed URLs too, so a dead link that many pages point to is fetched once, not once per page; retry failures explicitly with `continue_run` (below). To deliberately re-collect a URL, register it with `overwrite`: `urls_to_overwrite=` or `register_links_to_handler(..., overwrite=True)`, which resets it to `todo`. URLs whose session broke (`Statuses.SESSION_BROKEN`) are retried automatically.

**Resuming and retrying:**

If a run was interrupted, URLs may be stuck in `processing`; failed URLs stay in their error state until retried. Resume with:

```python
project.continue_run(
    inprogress=True,      # retry URLs stuck in processing
    conn_error=True,      # retry connection errors
    parsing_error=False,  # skip parsing errors (default)
)
```

### Data Depot

The depot stores all collected data as immutable, versioned runs. Each `commit_current_run()` archives the run and updates the status index.

**Directory layout:**

```
<depot root>/my-project/
├── object-store/        # content-addressed blob storage
│   ├── staging/         #   objects of the uncommitted run
│   └── ark/             #   committed objects (dirark: index.sqlite + tar.zst)
├── runs/                # archived collection runs
│   └── <run-name>/
│       ├── context.json   # git commit, environment snapshot
│       └── events.zip     # serialized collection events
├── status.sqlite.zip    # materialized source-URL status (zipped sqlite)
├── status-runs.json     # run ids already folded into status.sqlite.zip
└── current-run/         # active run (cleared on commit)
    ├── context.json
    ├── events/
    └── db.sqlite
```

**Reading collected data:**

```python
depot = project.depot

for event in depot.get_handler_events():
    print(event.url)
    print(event.handler_name)
    data = event.content   # whatever parse() returned
```

**Filtering events:**

`get_handler_events` selects and filters; each yielded `ParsedCollectionEvent` exposes `.url`, `.handler_name`, `.status`, and `.content`. Pass `from_current=True` to read the uncommitted (or test) run instead of the committed history.

```python
# successful events for one handler, latest per URL (the defaults)
for event in depot.get_handler_events(ProductHandler):
    data = event.content

# include failures, keep every occurrence, restrict to the last 2 runs
for event in depot.get_handler_events(
    ProductHandler, only_successful=False, only_latest=False, past_runs=2
):
    print(event.status, event.url)
```

### Remote Sync

Sync a depot with a remote copy for distributed collection or backup. Push/pull shell out to the system `rsync` binary (Linux-only by design), so any target `rsync` understands works — a local path or `user@host:/path`; the depot lives at `<remote>/<depot name>`.

Both directions only add what the other side lacks. Runs never change, and the status files are a cache that is repaired by replaying the runs it is missing, so both transfer without overwriting. Committed objects sync through dirark's index-first merge: only objects the other side lacks move, and objects pushed from different machines all survive. `current-run/` and objects of the uncommitted run stay local. Pulling a depot the remote does not hold yet is a no-op. Two machines must not push to the same remote at the same moment.

Set a default target with `ASWAN_REMOTE`, or pass it explicitly:

```bash
export ASWAN_REMOTE=user@host:/srv/aswan-depots
```

```python
project.depot.pull()                 # pull from $ASWAN_REMOTE before running
project.run(...)
project.commit_current_run()
project.depot.push()                 # push results back

project.depot.push("user@host:/srv/aswan-depots")  # or target explicitly
```

To merge depots collected independently (e.g. one per machine) without a shared remote, use `target.merge(other)`.

### Convenience Functions

For one-off fetching without setting up a project:

```python
from aswan import get_soup, get_json

soup = get_soup("https://example.com")
data = get_json("https://api.example.com/endpoint")

# With browser
soup = get_soup("https://example.com", browser=True, headless=True)
```

## Handler Reference

### Class attributes

| Attribute | Type | Description |
| --- | --- | --- |
| `url_root` | `str \| None` | Base URL for resolving relative links |
| `test_urls` | `list[str]` | URLs used during `test_run=True` |
| `proxy_cls` | `type[ProxyBase]` | Proxy rotation for this handler (see Proxies) |
| `max_in_parallel` | `int \| None` | Max concurrent requests for this handler |
| `max_retries` | `int` | Attempts per URL, the first one included, before it is marked failed |
| `restart_session_after` | `int` | Restart the session after this many requests |
| `headless` | `bool` | Browser handlers: run Chrome without a window |
| `eager` | `bool` | Browser handlers: eager page-load strategy |

### Methods to override

| Method | Signature | Description |
| --- | --- | --- |
| `parse(content)` | `(soup/dict/bytes) -> Any` | Extract and return data from fetched content |
| `handle_driver(session)` | `(Session or Chrome) -> Any` | Request handlers: runs before each request, e.g. to set headers or cookies. Browser handlers: runs after the page loads; a non-`None` return replaces the page source as the content passed to `parse` |
| `start_session(session)` | `(Session or Chrome) -> None` | One-time session initialization |
| `is_session_broken(result)` | `(int or Exception) -> bool` | Whether a failure warrants restarting the session |
| `load_cache(url)` | `(str) -> Any \| None` | Return pre-collected data to skip the fetch (and `parse`); status becomes `CACHE_LOADED`. Useful for offline replay or seeding a depot with results gathered elsewhere |

### Registering URLs from within parse()

```python
def parse(self, soup):
    # register to same handler
    self.register_links_to_handler(["/page/2", "/page/3"])

    # register to a different handler
    self.register_links_to_handler(
        ["/item/42"], handler_cls=DetailHandler
    )

    # register with overwrite (re-collect even if already done)
    self.register_links_to_handler(["/dynamic"], overwrite=True)

    # register URL with query params
    self.register_url_with_params({"page": 2, "limit": 50})

    return soup.get_text()
```

## Status Values

| Constant | Value | Meaning |
| --- | --- | --- |
| `Statuses.TODO` | `todo` | Pending collection |
| `Statuses.PROCESSING` | `processing` | Currently being collected |
| `Statuses.PROCESSED` | `D` | Successfully collected |
| `Statuses.PARSING_ERROR` | `PE` | `parse()` raised an exception |
| `Statuses.CONNECTION_ERROR` | `CE` | Network or HTTP failure |
| `Statuses.SESSION_BROKEN` | `SB` | Session needs restart |
| `Statuses.CACHE_LOADED` | `CL` | Returned by `load_cache()` |

## Monitoring

With the `monitor` extra, a Dash dashboard shows the current run of a depot: collection rates, handler status breakdowns, and recent events.

```bash
uv run aswan monitor my-project    # --root DIR, --port N, --interval SECS, --debug
```

## Migrating a pre-1.0 depot

A depot written before 1.0 keeps one zip file per object and YAML contexts, which the current object store cannot read. `aswan migrate` copies its runs into a current depot, re-storing every object the events name and rewriting their names:

```bash
uv run --extra legacy aswan migrate ~/old-depots/my-project ~/aswan-depots/my-project
```

The target is created if missing. Runs it already holds are skipped, so the same command resumes an interrupted migration, pulls new runs from a still-legacy live depot, and merges several copies of one depot into one target. The events of the legacy `current-run` become a run of their own. A run with a missing or unreadable object is not written: the summary lists it, and the command exits non-zero.

## Proxies

```python
from aswan import ProxyAuth, ProxyBase

class MyProxy(ProxyBase):
    port_no = 8080

    def _load_host_list(self) -> list[str]:
        return ["proxy1.example.com", "proxy2.example.com"]  # rotated per session

    def get_creds(self) -> ProxyAuth | None:
        return ProxyAuth(user="u", password="p")  # omit/return None for no auth

class MyHandler(RequestSoupHandler):
    proxy_cls = MyProxy
```

## Time Constants

```python
from aswan import ONE_MINUTE, ONE_HOUR, ONE_DAY, ONE_WEEK, ONE_YEAR
```

These are integer values in seconds, useful for scheduling and TTL logic.

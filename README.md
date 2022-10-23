# aswan

[![Documentation Status](https://readthedocs.org/projects/aswan/badge/?version=latest)](https://aswan.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/endremborza/aswan.svg)](https://codeclimate.com/github/endremborza/aswan)
[![codecov](https://img.shields.io/codecov/c/github/endremborza/aswan)](https://codecov.io/gh/endremborza/aswan)
[![pypi](https://img.shields.io/pypi/v/aswan.svg)](https://pypi.org/project/aswan/)

collect and organize data into a T1 data depot 
named after the [Aswan Dam](https://en.wikipedia.org/wiki/Aswan_Dam)

Collect and compress data from the internet for later parsing

- quick, parallel, customizable to collect
- compressed to store
- quick to sync with a remote store
  - sync to continue collecting
  - sync to parse  
- immutable collection

## Concepts

- objects
  - saved by collection events
- events
  - collection
  - registration (v2: registration for parsing)
  - (v2) parsing
- runs
  - manual run vs automated run
    - makes manual adding of urls easy but revertible
  - has unique id
  - generates events
  - linked to a specific version of the code
    - ideally commit hash + pip freeze
- statuses
  - determined by base status + runs integrated
  - contains
    - what urls need to be collected
    - (v2) what collected objects need to be parsed
  - sqlite file, constantly trimmed

### Structure

- objects
  - 00, 01, ...
- runs
  - run-hash
      - context.yaml
        - commit-hash, pip-freeze, ...
      - events.zip
- statuses
  - status-hash
    - context.yaml
      - parent-status, integrated
    - db.sqlite.zip
- current-run
  - context.yaml
  - events
    - these to be compressed into ../runs
  - status.sqlite

- there is a 'TEST' status
  - cannot be integrated whatever is based on it
  - a test run can be made on it...



when starting a run:
  - check if current-run is empty
    - if not, fail with 
  - find latest status
    - if it has not integrated all past runs, create a new status that has
  - start collection (+ registration)
  - either stops or breaks, all events and objects are saved to disk
  - if properly stops, move and compress stuff
    - based on one that was the starter, and current run id


## Pre v1.0 laundry list

- proxy auth test
- session break tests
- push/pull tests


git tag --sort=committerdate


- parsing/connection error confusion
  - also broken session thing
- conn session cpu requirement
- resource limits
- transferring / ignoring cookies


- template projects
  - oddsportal
    - updating thingy, based on latest match in season
  - footy
  - rotten
  - boxoffice

# aswan

[![Documentation Status](https://readthedocs.org/projects/aswan/badge/?version=latest)](https://aswan.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/endremborza/aswan.svg)](https://codeclimate.com/github/endremborza/aswan)
[![codecov](https://img.shields.io/codecov/c/github/endremborza/aswan)](https://codecov.io/gh/endremborza/aswan)
[![pypi](https://img.shields.io/pypi/v/aswan.svg)](https://pypi.org/project/aswan/)

collect and organize data into a T1 data lake and T2 tables. 
named after the [Aswan Dam](https://en.wikipedia.org/wiki/Aswan_Dam)

## Quickstart



## Pre v1.0 laundry list

will probably need to separate a few things from it:
- t2extractor
  - unstructured json to tabular data automatically
  - aswan.t2.extractor
- scheduler

TODO
- dvc integration
- export to dataset template
  - maybe part of the dataset
- cleanup requirements
- s3, scp for push/pull
- add verified invalid output that is not parsing error
- selective push / pull
  - with possible nuking of remote archive
  - cleaning local obj store (when envs blow up, ide dies)
- parsing/connection error confusion
  - also broken session thing
- conn session cpu requirement
- resource limits
- transfering / ignoring cookies
- lots of things with extractors
- template projects
  - oddsportal
    - updating thingy, based on latest match in season
  - footy
  - rotten
  - boxoffice

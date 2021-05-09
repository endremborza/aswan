# aswan

[![Documentation Status](https://readthedocs.org/projects/aswan/badge/?version=latest)](https://aswan.readthedocs.io/en/latest)
[![codeclimate](https://img.shields.io/codeclimate/maintainability/endremborza/aswan.svg)](https://codeclimate.com/github/endremborza/aswan)
[![codecov](https://img.shields.io/codecov/c/github/endremborza/aswan)](https://codecov.io/gh/endremborza/aswan)
[![pypi](https://img.shields.io/pypi/v/aswan.svg)](https://pypi.org/project/aswan/)

collect and organize data into a T1 data lake and T2 tables. 
named after the [Aswan Dam](https://en.wikipedia.org/wiki/Aswan_Dam)

## Quickstart


```python
import aswan

config = aswan.AswanConfig.default_from_dir("imdb-env")

celeb_table = config.get_prod_table("person")
movie_table = config.get_prod_table("movie")

project = aswan.Project(config) # this creates the env directories by default

@project.register_handler
class CelebHandler(aswan.UrlHandler):
    url_root = "https://www.imdb.com"

    def parse_soup(self, soup):
        return {
            "name": soup.find("h1").find("span").text.strip(),
            "dob": soup.find("div", id="name-born-info").find("time")["datetime"],
        }

@project.register_handler
class MovieHandler(aswan.UrlHandler):
    url_root = "https://www.imdb.com"
    def parse_soup(self, soup):

        for cast in soup.find("table", class_="cast_list").find_all("td", class_="primary_photo")[:3]:
            link = cast.find("a")["href"]
            self.register_link_to_handler(link, CelebHandler)
        
        return {
            "title": soup.find("title").text.replace(" - IMDb", "").strip(),
            "summary": soup.find("div", class_="summary_text").text.strip(),
            "year": int(soup.find("span", id="titleYear").find("a").text),
        }


# all this registering can be done simpler :)
project.register_t2_table(celeb_table)
project.register_t2_table(movie_table)

@project.register_t2_integrator
class MovieIntegrator(aswan.FlexibleDfParser):
    handlers = [MovieHandler]
    def get_t2_table(self):
        return movie_table

@project.register_t2_integrator
class CelebIntegrator(aswan.FlexibleDfParser):
    handlers = [CelebHandler]
    def get_t2_table(self):
        return celeb_table

def add_init_urls():
    movie_urls = [
        "https://www.imdb.com/title/tt1045772",
        "https://www.imdb.com/title/tt2543164",
    ]

    person_urls = ["https://www.imdb.com/name/nm0000190"]
    project.add_urls_to_handler(MovieHandler, movie_urls)
    project.add_urls_to_handler(CelebHandler, person_urls)

add_init_urls()

project.run(with_monitor_process=True)
```

```
    2021-05-09 22:13.42 [info     ] running function reset_surls   env=prod function_batch=run_prep
    ...
    2021-05-09 22:13.45 [info     ] ray dashboard: http://127.0.0.1:8266
    ...
    2021-05-09 22:13.45 [info     ]  monitor app at: http://localhost:6969
    ...
```

```python
movie_table.get_full_df()
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>title</th>
      <th>summary</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>I Love You Phillip Morris (2009)</td>
      <td>A cop turns con man once he comes out of the c...</td>
      <td>2009</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Arrival (2016)</td>
      <td>A linguist works with the military to communic...</td>
      <td>2016</td>
    </tr>
  </tbody>
</table>


```python
celeb_table.get_full_df()
```
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>dob</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Matthew McConaughey</td>
      <td>1969-11-4</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Leslie Mann</td>
      <td>1972-3-26</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Jeremy Renner</td>
      <td>1971-1-7</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Forest Whitaker</td>
      <td>1961-7-15</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Jim Carrey</td>
      <td>1962-1-17</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Amy Adams</td>
      <td>1974-8-20</td>
    </tr>
    <tr>
      <th>0</th>
      <td>Ewan McGregor</td>
      <td>1971-3-31</td>
    </tr>
  </tbody>
</table>




## Pre v0.0.0 laundry list

will probably need to separate a few things from it:
- t2extractor
- scheduler

TODO
- cleanup reqirements
- s3, scp for push/pull
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

```python
import pandas as pd

from bs4 import BeautifulSoup

import aswan
```


```python
project = aswan.Project("imdb-example")
```


```python
@project.register_handler
class CelebHandler(aswan.RequestSoupHandler):
    url_root = "https://www.imdb.com"

    def parse(self, soup: BeautifulSoup):
        return {
            "name": soup.find("h1").find("span").text.strip(),
            "dob": soup.find("div", id="name-born-info").find("time")["datetime"],
        }
```


```python
@project.register_handler
class MovieHandler(aswan.RequestSoupHandler):
    url_root = "https://www.imdb.com"

    def parse(self, soup: BeautifulSoup):

        for cast in soup.find("table", class_="cast_list").find_all(
            "td", class_="primary_photo"
        )[:3]:
            self.register_links_to_handler([cast.find("a")["href"]], CelebHandler)

        ref_section = soup.find("section", class_="titlereference-section-overview")
        summary = None
        if ref_section is not None:
            summary = getattr(ref_section.find("div"), "text", "").strip()
        return {
            "title": soup.find("title")
            .text.replace(" - Reference View - IMDb", "")
            .strip(),
            "summary": summary,
            "year": int(
                soup.find("span", class_="titlereference-title-year").find("a").text
            ),
        }
```


```python
project.run(
    urls_to_register={
        MovieHandler: [
            "https://www.imdb.com/title/tt1045772/reference",
            "https://www.imdb.com/title/tt2543164/reference",
        ],
        CelebHandler: ["https://www.imdb.com/name/nm0000190"],
    },
    force_sync=True
)
```

    2022-10-06 16:47.56 [info     ] running function setup         batch=prep
    2022-10-06 16:47.56 [info     ] function setup returned None   batch=prep
    2022-10-06 16:47.56 [info     ] running function _initiate_status batch=prep
    2022-10-06 16:47.56 [info     ] function _initiate_status returned None batch=prep
    2022-10-06 16:47.56 [info     ] running function _create_scheduler batch=prep
    2022-10-06 16:47.56 [info     ] function _create_scheduler returned None batch=prep
    2022-10-06 16:48.12 [info     ] running function join          batch=cleanup
    2022-10-06 16:48.12 [info     ] function join returned None    batch=cleanup



```python
pd.DataFrame([pcev.content for pcev in project.handler_events(MovieHandler)])
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
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
      <th>1</th>
      <td>Arrival (2016)</td>
      <td>A linguist works with the military to communic...</td>
      <td>2016</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.DataFrame([pcev.content for pcev in project.handler_events(CelebHandler)])
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
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
      <td>Ewan McGregor</td>
      <td>1971-3-31</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jeremy Renner</td>
      <td>1971-1-7</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Leslie Mann</td>
      <td>1972-3-26</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Forest Whitaker</td>
      <td>1961-7-15</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Amy Adams</td>
      <td>1974-8-20</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Jim Carrey</td>
      <td>1962-1-17</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Matthew McConaughey</td>
      <td>1969-11-4</td>
    </tr>
  </tbody>
</table>
</div>




```python
project.cleanup_current_run()
```


```python
project.depot.purge()
```

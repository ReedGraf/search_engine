> ⚠️ This project is in a very primative state 🚧

# 🔍 Stultus - The Stupid Search Engine

> _Stultus is latin for "stupid"_

Just for fun, to learn about how web crawling, tokenizing, databases, server hardware, all that works in a real project.

Just to build something cool


## Indexes to index:

- Kagi list of small blogs (these are rss files, need to parse those) - https://github.com/kagisearch/smallweb/blob/main/smallweb.txt
- DNS records of all URLs [https://domainsproject.org/dataset](https://domainsproject.org/dataset)


## How to set up for Windows:

```
python -m venv venv
venv\Scripts\Activate
$env:DATABASE_URL = "postgres://postgres:password@172.233.221.151:5432/search_engine
pip install -r requirements.txt
```

## Project Layout:

```
search.py          Searching functions & algorithms
scrape.py          Main scraping
└ scraper.py       Functions called to scrape
  └ tokenizer.py   Tokenize the scraped data to
                   be added to the database
```

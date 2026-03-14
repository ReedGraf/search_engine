# 🔍 Stultus - The Stupid Search Engine

> _Stultus is latin for "stupid"_

Stultus is a full search engine system made by UWEC students with hand built internet crawler, tokenization, and search algorithm. This all started as a project to learn about how web crawling, tokenizing, databases and server hardware.

And to build something cool. 🔥

## Indexes to index

- Kagi list of small blogs (these are rss files, need to parse those) - https://github.com/kagisearch/smallweb/blob/main/smallweb.txt
- DNS records of all URLs [https://domainsproject.org/dataset](https://domainsproject.org/dataset)

## Development

### Setup Development Environment

```bash
# Linux
git clone https://github.com/ThisIsNotANamepng/search_engine.git
cd search_engine
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
DATABASE_URL="postgres://postgres:postgressPassword@postgressIP:5432/search_engine" python3 fileNameHere.py
```

```powershell
# Windows
# you can't scrape on Windows, the scraper uses a function only available on Linux
git clone https://github.com/ThisIsNotANamepng/search_engine.git
cd search_engine
python -m venv venv
venv\Scripts\Activate
pip install -r requirements.txt
$env:DATABASE_URL = "postgres://postgres:postgressPassword@postgressIP:5432/search_engine" 
```

### Project Layout

```
app.py             Dashboard & searching pages
search.py          Searching functions & algorithms
scrape.py          Main scraping script
└ scraper.py       Functions called to scrape
  └ tokenizer.py   Tokenize the scraped data to
                   be added to the database
seed_urls.csv      Website URLs to jumpstart 
                   the crawler queue
```


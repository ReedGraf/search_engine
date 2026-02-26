"""
For testing different search algorithms and tokenizing and weights
"""

### BASE FUNCTIONS

import sqlite3, os
import scraper
import tokenizer

DATABASE = "search_database.db"
TEXTS_FILEPATH = "/texts/wikipedia/"

def create_database():
        """
        Creates the test database
        """

        # The SQL schema converted for SQLite3
        schema_sql = """
        CREATE TABLE IF NOT EXISTS words (
            word TEXT NOT NULL PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bigrams (
            bigram TEXT PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trigrams (
            trigram TEXT PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prefixes (
            prefix TEXT NOT NULL PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS urls (
            url TEXT NOT NULL PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS urls_references (
            url TEXT NOT NULL PRIMARY KEY,
            referenced_domain TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS total_references (
            domain TEXT NOT NULL PRIMARY KEY,
            total_references INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS entities (
            entity_text TEXT NOT NULL PRIMARY KEY,
            id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS entity_urls (
            url TEXT NOT NULL PRIMARY KEY,
            entity_url INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bigram_urls (bigram_id INTEGER NOT NULL, url_id INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS trigram_urls (trigram_id INTEGER NOT NULL, url_id INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS prefix_urls (prefix_id INTEGER NOT NULL, url_id INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS word_urls (word_id INTEGER NOT NULL, url_id INTEGER NOT NULL);

        CREATE TABLE IF NOT EXISTS weights (type TEXT PRIMARY KEY, weight REAL NOT NULL);

        CREATE TABLE IF NOT EXISTS url_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip TEXT,
            message TEXT
        );
        """

        try:
            # Connect to the database (creates file if it doesn't exist)
            with sqlite3.connect(DATABASE) as conn:
                # Create a cursor object
                cursor = conn.cursor()

                # Execute the entire schema script
                cursor.executescript(schema_sql)

                # Commit the changes
                conn.commit()

        except sqlite3.Error as e:
            print(f"An error occurred while creating the database: {e}")


def load_texts_into_db():
    a=1


create_database()

### OUR CURRENT SOLUTION

# Base create database
def add_texts_to_db():
    for i in os.listdir(TEXTS_FILEPATH):

        with open(i, "r") as file:
            text = file.read()

        tokens = tokenizer.tokenize_all(text) #TODO: I want to see how expensive it is to check if each token is in the database already, you'd have to search all of the tokens to find if it's in the database



        # tokens: [words, bigrams, trigrams, prefixes]
        words = set(tokens[0]) if tokens and len(tokens) > 0 else set()
        bigrams = set(tokens[1]) if tokens and len(tokens) > 1 else set()
        trigrams = set(tokens[2]) if tokens and len(tokens) > 2 else set()
        prefixes = set(tokens[3]) if tokens and len(tokens) > 3 else set()



        conn = get_conn()
        cur = conn.cursor()



        # Bulk insert words/bigrams/trigrams/prefixes using execute_values for speed.
        if words:
            extra_vals = [(w,) for w in words]
            extras.execute_values(cur,
                "INSERT INTO words (word) VALUES %s ON CONFLICT (word) DO NOTHING;",
                extra_vals,
                template=None)

        if bigrams:
            extra_vals = [(b,) for b in bigrams]
            extras.execute_values(cur,
                "INSERT INTO bigrams (bigram) VALUES %s ON CONFLICT (bigram) DO NOTHING;",
                extra_vals)

        if trigrams:
            extra_vals = [(t,) for t in trigrams]
            extras.execute_values(cur,
                "INSERT INTO trigrams (trigram) VALUES %s ON CONFLICT (trigram) DO NOTHING;",
                extra_vals)

        if prefixes:
            extra_vals = [(p,) for p in prefixes]
            extras.execute_values(cur,
                "INSERT INTO prefixes (prefix) VALUES %s ON CONFLICT (prefix) DO NOTHING;",
                extra_vals)
            
        if links:
            extra_vals = [(url, l) for l in links]
            extras.execute_values(cur,
                "INSERT INTO urls_references (url, referenced_domain) VALUES %s ON CONFLICT (url) DO NOTHING;",
                extra_vals,
                template=None)


        # Fetch ids for all tokens in bulk
        def fetch_id_map(column, table, items):
            if not items:
                return {}
            cur.execute(sql.SQL("SELECT id, {col} FROM {tbl} WHERE {col} = ANY(%s);").format(
                col=sql.Identifier(column), tbl=sql.Identifier(table)
            ), (list(items),))
            rows = cur.fetchall()
            return {val: id for (id, val) in rows}
        

        word_map = fetch_id_map('word', 'words', list(words))
        bigram_map = fetch_id_map('bigram', 'bigrams', list(bigrams))
        trigram_map = fetch_id_map('trigram', 'trigrams', list(trigrams))
        prefix_map = fetch_id_map('prefix', 'prefixes', list(prefixes))


        # Prepare mapping inserts and bulk insert them
        word_url_pairs = [(word_map[w], url_id) for w in words if w in word_map]
        bigram_url_pairs = [(bigram_map[b], url_id) for b in bigrams if b in bigram_map]
        trigram_url_pairs = [(trigram_map[t], url_id) for t in trigrams if t in trigram_map]
        prefix_url_pairs = [(prefix_map[p], url_id) for p in prefixes if p in prefix_map]


        if word_url_pairs:
            extras.execute_values(cur,
                "INSERT INTO word_urls (word_id, url_id) VALUES %s;",
                word_url_pairs)

        if bigram_url_pairs:
            extras.execute_values(cur,
                "INSERT INTO bigram_urls (bigram_id, url_id) VALUES %s;",
                bigram_url_pairs)

        if trigram_url_pairs:
            extras.execute_values(cur,
                "INSERT INTO trigram_urls (trigram_id, url_id) VALUES %s;",
                trigram_url_pairs)

        if prefix_url_pairs:
            extras.execute_values(cur,
                "INSERT INTO prefix_urls (prefix_id, url_id) VALUES %s;",
                prefix_url_pairs)


        conn.commit()
        cur.close()
        conn.close()

# Searching v1.0
def search(query):
    a=1
    

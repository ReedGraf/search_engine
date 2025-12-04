# Search Engine

import tokenizer
import scraper
import sqlite3
import time

DATABASE_PATH = "database.db"

def search(query):

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    weights = cursor.execute("SELECT * FROM weights;").fetchall()
    

    print(weights)
    word_weight = (weights[0][1])
    bigram_weight = (weights[1][1])
    trigram_weight = (weights[2][1])
    prefix_weight = (weights[3][1])

    tokenized = tokenizer.tokenize_all(query)

    # Build placeholder groups
    word_q = ",".join(["?"] * len(tokenized[0]))
    bigram_q = ",".join(["?"] * len(tokenized[1]))
    trigram_q = ",".join(["?"] * len(tokenized[2]))
    prefix_q = ",".join(["?"] * len(tokenized[3]))

    sql_query = f"""
    WITH scores AS (
        -- Words
        SELECT url_id,
            COUNT(*) * {word_weight} AS score
        FROM word_urls
        WHERE word_id IN (
            SELECT id FROM words
            WHERE word IN ({word_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Bigrams
        SELECT url_id,
            COUNT(*) * {bigram_weight} AS score
        FROM bigram_urls
        WHERE bigram_id IN (
            SELECT id FROM bigrams
            WHERE bigram IN ({bigram_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Trigrams
        SELECT url_id,
            COUNT(*) * {trigram_weight} AS score
        FROM trigram_urls
        WHERE trigram_id IN (
            SELECT id FROM trigrams
            WHERE trigram IN ({trigram_q})
        )
        GROUP BY url_id

        UNION ALL

        -- Prefixes
        SELECT url_id,
            COUNT(*) * {prefix_weight} AS score
        FROM prefix_urls
        WHERE prefix_id IN (
            SELECT id FROM prefixes
            WHERE prefix IN ({prefix_q})
        )
        GROUP BY url_id
    )

    SELECT urls.url,
        SUM(score) AS score
    FROM scores
    JOIN urls ON urls.id = scores.url_id
    GROUP BY urls.id
    ORDER BY score DESC
    LIMIT 10;
    """

    # Same params order as before, just concatenated
    params = (
        tokenized[0] +
        list(tokenized[1]) +
        list(tokenized[2]) +
        list(tokenized[3])
    )

    results = cursor.execute(sql_query, params).fetchall()

    print(results)

    connection.close()


#query=input("Search query: ")
#query = "How do I hack a website"
#search(query)

#import os
#os.system("rm database.db")
create_database()
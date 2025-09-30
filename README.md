# Assignment3-CS121

### GENERATING THE INDEX ###
Create the partial inverted indices of the web documents by running "python3 indexer/indexer.py"
Merge the partial indices by running "python3 indexer/reader.py"
Filter the inverted index by running "python3 indexer/filterer.py"
Generate a text file from the shelved inverted index by running "python3 indexer/index_txt.py"

Generate a text file mapping doc IDs to their word counts by running "python3 indexer/counter.py"
Generate a text file mapping doc IDs to their urls by running "python3 utils/identifier.py"

Generate a shelve mapping doc IDs to their found outgoing links by running "python3 indexer/outer.py"
Generate a two text files containing authority information by running "python3 indexer/authoritator.py"

### RUNNING THE SEARCH ENGINE ###
To launch the search engine, run "python3 indexer/boolean.py"
    note: it's called boolean.py but it's not a boolean retrieval engine (anymore)

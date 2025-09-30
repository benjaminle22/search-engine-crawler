import indexer.indexer as i
import shelve as s
from time import sleep

def main() -> None:
    i.generate_inverted_index(i.TINYDEV_PATH)
    with s.open(i.INVERTED_INDEX_FILE_NAME) as omega:
        for token, postings in omega.items():
            print(f"TOKEN: {token}\nPOSTINGS: {postings}")

if __name__ == '__main__':
    main()
from typing import Dict
from bs4 import BeautifulSoup as BS
from urllib.parse import urlparse, urlunparse, urldefrag, urljoin
import shelve
import os

class Document:
    _docID = 0 # initalizes a document ID (Serial #) for every document


class Posting:
    """A class to represent a Posting.
    
    Parameters:
        document_id: the ID of the document this Posting is for
        term_frequency: the frequency of the Term in the document with id document_id
        position: postional datat of the term in the document (TBD)
    """
    def __init__(self, document_id:int, term_frequency:int, positions:list):
        self.document_id = document_id
        self.term_frequency = term_frequency
        self.positions = positions
        # anything else?
    
    def __lt__(self, other):
        # Only works with another Posting instance.
        if not isinstance(other, type(self)):
            raise NotImplemented
        return self.document_id < other.document_id


    def __repr__(self):
        return f"Posting(document_id={self.document_id}, term_frequency={self.term_frequency}, positions={self.positions})"


# TOKEN FILE PARSING

def tokenize(file_path: str) -> list[str]:
    """
    Given a text file path, lex the contents
    and return a list of alphanumeric tokens

    Runtime:
        O(n) with n being the size of the file in bytes
    Justification:
        We are iterating through the contents of the file 100 bytes at a time,
        processing the data as we go.
    """
    # List of alphanumeric strings
    tokens = []

    try:
        # Open the file in read mode with UTF-8 encoding to prevent text data corruption
        with open(file_path, 'r', encoding='UTF-8', errors='ignore') as file:
            # Read the first 100 bytes from the file
            buffer = file.read(100)

            # Builder string to keep track of the alphanumeric sequence of characters
            builder = ""

            # Loop until the buffer string is empty
            while len(buffer) != 0:
                # Check if each character is alphanumeric
                for char in buffer:
                    if is_alnum(char): # if character is alphanumeric
                        builder += char # append the character into the builder
                    else: # if character is NOT alphanumeric and the builder string is NOT empty
                        if builder.strip():
                            tokens.append(builder.strip().lower()) # add the builder string into the list of tokens
                        builder = "" # flush the builder string
                buffer = file.read(100) # read the next 100 bytes

            # If we hit EOF, there may be data in the builder string
            if len(builder) != 0: # if so
                tokens.append(builder) # add builder string into list of tokens
    except FileNotFoundError: # handling nonexistent files
        print(f'File does not exist!')
        return []
    except IOError as e: # handling any IO error
        print(f'Something went wrong: {e}')
        return []

    return tokens


def is_alnum(char: str) -> bool:
    """
    Given a single character,
    determine if it is alphanumeric

    Runtime:
        O(1)
    Justification:
        Simple boolean check.
    """
    char_ord = ord(char)
    return (48 <= char_ord <= 57) or \
        (65 <= char_ord <= 90) or \
        (97 <= char_ord <= 122)


def compute_word_frequencies(tokens: list[str]) -> Dict[str, int]:
    """
    Given a list of Tokens,
    return a dictionary mapping a token to its frequency count

    Runtime:
        O(n) with n being the size of the list of tokens
    Justification:
        We are iterating through a list of tokens
        and doing work that costs O(1) per iteration.
    """
    # Initialize a dictionary variable
    frequencies = dict()

    # Iterate through each of the tokens
    for token in tokens:
        if token in frequencies: # if the token is already in the dictionary
            frequencies[token] += 1 # increment the count of the token
        else:
            frequencies[token] = 1 # initialize the key to value of 1

    return frequencies

def post_tokens(doc_id: int, tokens: list) -> dict:
    """creates Postings for each (unqiue) term found in a page's content.
    The raw list of tokens is used to discover the positions of each token in the page.
    Returns a mapping between tokens and Postings.
    """
    postings = dict()
    for index, token in enumerate(tokens):
        if token in postings:
            postings[token].term_frequency += 1
        else:
            postings[token] = Posting(doc_id, 1, list())
        postings[token].positions.append(index)
    return postings


# URL PARSING

def normalize(url: str) -> str:
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme
    if scheme == 'http': scheme = 'https'
    host = parsed_url.netloc.split('@')[-1].split(':')[0]
    path = parsed_url.path
    return urlunparse((scheme, host, path, '', '', '')).rstrip("/")

def absolutize(doc_url: str, url: str) -> str:
    parsed_url = urlparse(url)
    absolute_url = None
    if not parsed_url.scheme:
        absolute_url = urljoin(doc_url, url)
    else:
        absolute_url = url
    return absolute_url



# MERGING INVERTED INDEX AND OUTGOING URL INDEX

FILE_NAME = "OMEGA_INDEX"

def merge_partial_indexes() -> None:
    """
    Merges the partial indexes into one big index.
    """
    if (os.path.exists(FILE_NAME)):
        os.remove(FILE_NAME)
    with shelve.open(FILE_NAME, writeback=True) as omega:
        for i in range(1, 14):
            with open("merge-log.txt", "a") as f:
                f.write(f"Now merging: {i}\n")
            with shelve.open(f"partials/{FILE_NAME}-{i}", writeback=True) as partial:
                for token in partial.keys():
                    if token in omega:
                        omega[token] += partial[token]
                    else:
                        omega[token] = partial[token]
        with open("merge-log.txt", "a") as f:
            f.write("Finished merging partial inverted indices!\n")

def merge_outgoing() -> None:
    with open("merge-log.txt", "a") as f:
        f.write(f"Now merging document outgoing links...\n")
    with shelve.open("OMEGA_OUTGOING", writeback=True) as omega:
        for i in range(1, 14):
            with open("merge-log.txt", "a") as f:
                f.write(f"Now merging: {i}\n")
            with shelve.open(f"outgoings/OUTGOING_LINKS-{i}", writeback=True) as partial:
                for doc in partial.keys():
                    print(doc)
                    omega[doc] = partial[doc]
        with open("merge-log.txt", "a") as f:
            f.write("Finished merging partial outgoing links shelves!\n")
        

def generate_report() -> None:
    with open("m1_report.txt", "w") as report:
        if (os.path.exists(FILE_NAME)):
            report.write(f"Number of documents: {55393}\n") # info from structurer.py
            report.write(f"Shelve size: {os.path.getsize(FILE_NAME) / 1024} KB\n")            
            with shelve.open(f"{FILE_NAME}") as omega:
                report.write(f"Number of unique tokens: {len(omega)}\n")
                for token in omega.keys():
                    report.write(f"{token} | {(', ').join([str(posting.document_id) for posting in omega[token]])}\n")
        else:
            raise FileNotFoundError("No OMEGA_INDEX!")

if __name__ == '__main__':
    merge_partial_indexes()
    # merge_outgoing()
    generate_report()
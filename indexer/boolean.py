"""For A3 M3."""
import shelve
import reader
import json 
import time
import math
import ranker
import heapq
from nltk.stem import PorterStemmer as PS

ID_TO_URL = dict()
ID_WORDCOUNT = dict()
K = 50
A = 300
ALPHA = 1.5 # tuning factor for computing relevance scores
BETA = 2.5 # tuning factor for computing relevance scores
# constants for how many results we return


def doc_intersection(list1:int, list2:int) -> list:
    """Computes the intersection of two lists
    of document IDs
    """
    res = [] # init empty list
    while list1 and list2: # iterate through both lists, building res if an element is shared between them
        if list1[-1] == list2[-1]:
            res.append(list1.pop())
            list2.pop()
        elif list1[-1] < list2[-1]:
            list1.pop()
        else:
            list2.pop()
    return res # return the list of shared elements


def _extract_index_of_index() -> dict[str:int]:
    """extracts the index of the index into a dictionary,
    a mapping between token:byte"""
    res = {}
    with open("index_of_index.txt", "r") as f:
        line = f.readline()
        while line:
            splitline = line.split("|")
            res[splitline[0].strip()] = int(splitline[1])
            line = f.readline()
    return res


def _term_idfs(index_file_seek:dict[str:int]) -> dict[str:int]:
    """pre-computes the idf for all terms in the index"""
    res = {}
    with open("index.txt", "r") as f:
        for term in index_file_seek:
            f.seek(index_file_seek[term])
            line = f.readline()
            line = line.split(" | ")
            res[term] = math.log(55393 / len([i for i in line[1:] if i and i != '\n']))
    return res


def _extract_postings_list(term:str, index_of_index:dict) -> list[int]:
    """retrieves the postings list for a given term
    result returned in the form [docID]
    """
    postings = list()
    with open("index.txt", "r") as f:
        f.seek(index_of_index[term])
        line = f.readline()
        line = line.split(" | ")
        for posting in line[1:]:
            if posting and posting != '\n':
                posting = posting.split("|")
                postings.append(int(posting[0]))
    return postings 


def _load_id_to_url():
    """loads data from id_to_url.txt
    into a in-memory variable"""
    with open("boolean-log.txt", "a") as log:
        log.write(f"loading id urls\n")
    with open('id-to-url.txt', 'r') as f:
        line = f.readline()
        while line:
            line = line.split()
            ID_TO_URL[int(line[0])] = line[1].strip()
            line = f.readline()
    with open("boolean-log.txt", "a") as log:
        log.write(f"completed loading id urls\n")


def _compute_tf_idfs(index_of_index:dict, id_wordcount:dict) -> None:
    """pre-computes all tf-idf scores for 
    every term against every doc in its postings list"""
    with open("boolean-log.txt", "a") as log:
        log.write(f"computing tf idf scores\n")
    with open('tf-idf.txt', 'a') as f:
        for term in index_of_index:
            for docID in _extract_postings_list(term, index_of_index):
                with open("boolean-log.txt", "a") as log:
                    log.write(f"{term}|{docID}\n")
                f.write(f"{term}|{docID}|{ranker.compute_tf_idf(term, docID, index_of_index, id_wordcount)}\n") 
    with open("boolean-log.txt", "a") as log:
        log.write(f"all scores computed!\n")


def _retrieve_tf_idf() -> dict[(str, int):float]:
    scores = dict()
    with open('tf-idf.txt', 'r') as f:
        line = f.readline()
        while line:
            line = line.split("|")
            scores[(line[0], int(line[1]))] = float(line[2])
            line = f.readline()
    return scores


def _get_doc_wordcount() -> dict[int:int]:
    """gets wordcount of each page.
    dict returned is docID:wordcount"""
    with open("boolean-log.txt", "a") as log:
        log.write(f"getting wordcounts\n")
    res = dict()
    with open("ID_WORDCOUNT.txt", "r") as f:
        line = f.readline()
        while line:
            line = line.split()
            res[int(line[0])] = int(line[1])
            line = f.readline()
    with open("boolean-log.txt", "a") as log:
        log.write(f"completd getting wordcounts\n")
    return res


def run_engine() -> None:
    """
    Engine for boolean retrieval
    """
    index_of_index = _extract_index_of_index()
    idfs = _term_idfs(index_of_index)
    _load_id_to_url()
    id_wordcount = _get_doc_wordcount()
    #_compute_tf_idfs(index_of_index, id_wordcount) # not needed once all scores computed 
    tf_idf_scores = _retrieve_tf_idf()
    page_ranks = ranker.compute_pagerank(index_of_index)
    stemmer = PS()
    while True: # infinite loop for input
        query = input("Enter query: ") # prompt input
        start_time = time.time()
        terms = query.lower().strip().split() # lowercase and tokenize by space the query
        terms = [stemmer.stem(term) for term in terms]
        val_terms = [term for term in terms if term in index_of_index]
        for term in val_terms:
            print(f"term {term}'s idf: {idfs[term]}")
            # todo: heuristic for high IDFs
        rel_scores = dict()
        low_idf_scores = sum(1 for term in val_terms if idfs[term] < 1)
        for term in set(val_terms):
            if idfs[term] < 1 and low_idf_scores / len(val_terms)  < 0.6:
                print("skipping ", term)
                continue # ignore low idf terms if they make up less than 60% of the query
            # todo: ignore docs with a 0 cosine sim
            s = time.time()
            term_postings = _extract_postings_list(term, index_of_index)
            e = time.time()
            print(f"time to extract postings: {(e - s) * 1000}ms")
            s = time.time()
            blacklist = set()
            for docID in term_postings:
                if len(rel_scores) == A:
                    break
                if docID not in rel_scores and docID not in blacklist:
                    result = ALPHA * page_ranks[docID] + BETA * ranker.compute_cosine_sim(val_terms, docID, idfs, index_of_index, tf_idf_scores)
                    # g(d) + cosine_sim(q, d), with tuning factors
                    if result:
                        rel_scores[docID] = result
                    else:
                        blacklist.add(docID)
            e = time.time()
            print(f"time to compute cosine score: {(e - s) * 1000}ms")
        s = time.time()
        score_heap = [(-score, docID) for docID, score in rel_scores.items() if score != 0]
        heapq.heapify(score_heap)
        e = time.time()
        print(f"time to get heap: {(e - s) * 1000}ms")
        results = [] # list of urls
        for _ in range(min(len(score_heap), K)):
            res = heapq.heappop(score_heap)
            print(res)
            results.append(ID_TO_URL[res[1]]) # shelve maps str(doc_id) to doc_url!
        #with open('log.txt', 'w') as f: # log results
            #f.write(f"---- Results for '{query}':\n")
        print(f"---- Results for '{query}':")
        for url in results: # print out all results
            #with open('log.txt', 'a') as f:
                #f.write(f"{url}\n")
            print(url)
        end_time = time.time()
        runtime_ms = (end_time - start_time) * 1000
        print("Runtime: {} milliseconds".format(runtime_ms))

if __name__ == '__main__':
    run_engine()
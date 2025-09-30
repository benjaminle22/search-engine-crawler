"""This module contains functions for computing the relevance score of a document."""
import math
import json


def _get_outgoing_link_stats() -> dict[int:int]:
    """gets the number of outgoing links for each page.
    returns dict in form[docID:numLinks]"""
    res = dict()
    with open("ID_NUM_OUTGOINGS.txt", "r") as f:
        line = f.readline()
        while line:
            line = line.split()
            res[int(line[0])] = int(line[1])
            line = f.readline()
    return res


def _get_incoming_link_stats() -> dict[int:list[int]]:
    """gest the (IDs of) incoming links for each page"""
    res = dict()
    with open("ID_INCOMING_LIST.txt", "r") as f:
        line = f.readline()
        while line:
            line = line.split("|")
            res[int(line[0])] = [int(docID) for docID in json.loads(line[1].strip())]
            line = f.readline()
    return res



def compute_pagerank(index_of_index:dict) -> dict:
    """Computes the pagerank scores of all pages based off 
    the outgoing & incoming links for each one.
    
    function computePageRank(pages, dampingFactor, numIterations):
    N = number of pages
    pageRank = initialize dictionary with page as key and 1/N as value for each page
    newPageRank = initialize dictionary with page as key and 0 as value for each page
    outgoingLinks = initialize dictionary with page as key and number of outgoing links as value

    for each page in pages:
        outgoingLinks[page] = count of outgoing links from page

    for i from 1 to numIterations:
        for each page in pages:
            newPageRank[page] = (1 - dampingFactor) / N
            for each incomingLink in getIncomingLinks(page):
                newPageRank[page] += dampingFactor * (pageRank[incomingLink] / outgoingLinks[incomingLink])

        for each page in pages:
            pageRank[page] = newPageRank[page]

    return pageRank
    """
    # use if pagerank.txt exists
    res = dict()
    with open('pagerank.txt', 'r') as f:
        line = f.readline()
        while line:
            line = line.split('|')
            res[int(line[0])] = float(line[1])
            line = f.readline()
    return res

    # use the following if it doesn't.
    N = 55393
    damping = 0.80
    pageRank = {i: 1/N for i in range(N+1)}
    newPageRank = {i: 0 for i in range(N+1)}
    outgoingLinks = _get_outgoing_link_stats()
    incomingLinks = _get_incoming_link_stats()

    for _ in range(N+1):
        print(_)
        for pageID in range(N+1):
            print(pageID)
            newPageRank[pageID] = (1 - damping) / N
            if pageID in incomingLinks:
                for linkID in incomingLinks[pageID]:
                    newPageRank[pageID] += damping * (pageRank[linkID] / outgoingLinks[linkID])
        for pageID in range(N+1):
            print(pageID)
            pageRank[pageID] = newPageRank[pageID]
    """
    with open('pagerank.txt', 'a') as f: # uncomment if file already exists
        for key, value in pageRank.items():
            f.write(f"{key}|{value}\n")"""
    return pageRank



def compute_tf_idf(term:str, docID:int, index_of_index:dict, id_wordcount:dict) -> float:
    """Computes the tf-idf score of a 
    term against a given doc.
    
    The following heuristics will be used:
    1. term frequency for tf:
        tf_{t,d} / len(d)
    2. idf = log(N/df_t)

    the result will be tf * idf for term t againt document d
    
    we'll define tf_{t,d} as the
    frequency of term t in d, and
    df_t as the # of documents that contain t
    (i.e. the length of its postings list)"""
    # note: term is assumed to have a corresponding entry in index_of_index
    file_seek = index_of_index[term]
    postings:dict[int:int] = dict()
    line = ""
    posting_count = 0
    with open("index.txt", "r") as f:
        # extracts the postings list from the index.txt file
        f.seek(file_seek)
        line = f.readline()
        line = line.split(" | ")
        for posting in line[1:]:
            if posting and posting != '\n':
                posting = posting.split("|")
                postings[int(posting[0])] = int(posting[1])
                posting_count += 1
    if docID not in postings: # if this term doesn't appear in document # docID, return 0
        return 0
    try:
        tf = postings[docID] / id_wordcount[docID]
    except ZeroDivisionError: # if wordcount is 0, it contributes nothing
        tf = postings[docID] / 999999
    idf = math.log(55393 / posting_count, 10)
    return tf * idf


def compute_cosine_sim(query:list[str], docID: int, docIDFs:dict, 
        index_of_index:dict, tf_idf_scores:dict):
    """computes cosine similarity between a query and a document
    uses the formula provided in slides (lec 21)"""
    query_count = dict()
    for word in query:
        if word in query_count:
            query_count[word] += 1
        else:
            query_count[word] = 1
    numerator = 0
    normalize_q = 0
    normalize_d = 0
    for word, count in query_count.items():
        qi = ((count) / len(query)) * docIDFs[word]
        try:
            di = tf_idf_scores[(word, docID)]
        except KeyError:
            di = 0
        numerator +=  qi * di
        normalize_q += qi ** 2
        normalize_d += di ** 2
    try:
        return numerator / (math.sqrt(normalize_q) * math.sqrt(normalize_d))
    except ZeroDivisionError: # if query and doc have zero terms in common, 0 is returned
        return 0 
    




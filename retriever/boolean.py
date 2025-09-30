"""For A3 M2."""
import shelve
import indexer.reader
import json 

ID_TO_FILE = dict()


def doc_intersection(list1:int, list2:int) -> list:
    """Computes the intersection of two lists
    of document IDs
    """
    res = [] # init empty list
    while list1 and list2: # iterate through both lists, building res if an element is shared between them
        if list1[0] == list2[0]:
            res.append(list1[0])
            list1.pop(0)
            list2.pop(0)
        elif list1[0] < list2[0]:
            list1.pop(0)
        else:
            list2.pop(0)
    return res # return the list of shared elements


def run_engine() -> None:
    """
    Engine for boolean retrieval
    """
    while True: # infinite loop for input
        query = input("Enter query: ") # prompt input
        terms = query.lower().strip().split() # lowercase and tokenize by space the query
        invalid_word_alert = False # invalid word flag
        postings = [] # list of list of postings
        with shelve.open("OMEGA_INDEX") as omega: # open inverted index
            for term in terms: # for every tokenized term
                if term in omega:
                    postings.append(omega[term]) # add posting list
                else:
                    # no results found
                    invalid_word_alert = True # otherwise set flag to true
        if invalid_word_alert: # if flag is true
            print("No results found!!!!")
            continue # reloop
        
        postings.sort() # sort list of list of postings by length

        if len(postings) == 0: #if no results found
            print("No results found!")
            continue # reloop
        intersection = [posting.document_id for posting in postings.pop(0)] # initialize intersection list
        while len(postings): # while the list of list of postings is not empty
            new_list = [posting.document_id for posting in postings.pop(0)] # intialize a second list to intersect
            intersection = doc_intersection(intersection, new_list) # intersect both lists of postings
        
        # retrieve urls from given doc_ids
        with shelve.open("ID_URL") as id_url:
            results = [] # list of urls
            for doc_id in intersection:
                results.append(id_url[str(doc_id)]) # shelve maps str(doc_id) to doc_url!
        with open('log.txt', 'w') as f: # log results
            f.write(f"---- Results for '{query}':\n")
        print(f"---- Results for '{query}':")
        for url in results: # print out all results
            with open('log.txt', 'a') as f:
                f.write(f"{url}\n")
            print(url)

if __name__ == '__main__':
    run_engine()
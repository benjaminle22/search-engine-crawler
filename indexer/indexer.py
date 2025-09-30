import shelve
import json
import reader as reader
import os
from shutil import rmtree as rmdir
from bs4 import BeautifulSoup as soup
import threading
import time
from nltk.stem import PorterStemmer as PS

"""Contains the source code for the Inverted Index"""

DEV_PATH = f"./DEV/"
TINYDEV_PATH = f"./TINYDEV/"
TEMP_FILE_NAME = f"content"
INVERTED_INDEX_FILE_NAME = f"OMEGA_INDEX"
OUTGOING_LINKS_SAVE_NAME = f"OUTGOING_LINKS"

"""
Note to TA: shelve is meets the memory/disk constraints of A3
Clotho AI:
No, opening a shelf using the `shelve` module in Python does not load its entire contents into memory.
Instead, it allows you to access the data on an as-needed basis, similar to a dictionary.
When you access a particular key in the shelf, the corresponding data is read from the disk.
This makes `shelve` suitable for working with large datasets because it doesn't require that all data be loaded into memory at once.
"""
ID_TO_FILE = dict()

def generate_inverted_index(start:int, end:int, tid:int) -> None:
    """
    Generates an inverted index for all pages with IDs in range [start, end]
    """

    # JSON keys: url, content (html), encoding
    print(f"Thread {tid} now running!")
    start_time = time.time()
    postings = dict() # initialize fast inverted index
    with open(f"loggers/logger-{tid}.txt", "a") as log:
        log.write(f"Began indexing at time {start_time}\n")
    stemmer = PS()
    with shelve.open(f"partials/{INVERTED_INDEX_FILE_NAME}-{tid}", writeback=True) as omega: # shelve will be slow frequency data
        for idx in range(start, end+1): # iterate through each file in the subdirectory
            data = dict()
            filepath = ID_TO_FILE[idx]
            with open(f"loggers/logger-{tid}.txt", "a") as log:
                log.write(f"Now reading {idx} at {filepath}\n")
            with open(filepath, "r") as json_file:
                data = json.load(json_file) # load json file as dictionary
            with open(f"contents/{TEMP_FILE_NAME}-{tid}.txt", 'w', encoding = 'UTF-8') as tempfile:
                soupified = soup(data['content'], features='lxml') # create soup object

                # write title text with weight 10
                title_elements = soupified.find_all(['title'])
                for element in title_elements:
                    tempfile.write(f"{element.get_text()}\n" * 9)
                del title_elements

                # write strong, bold, italic text with weight 2
                bold_elements = soupified.find_all(['strong', 'b', 'i'])
                for element in bold_elements:
                    tempfile.write(f"{element.get_text()}\n" * 1)
                del bold_elements

                # write h1 text with weight 7
                h1_elements = soupified.find_all(['h1'])
                for element in h1_elements:
                    tempfile.write(f"{element.get_text()}\n" * 6)
                del h1_elements

                # write h2 text with weight 6
                h2_elements = soupified.find_all(['h2'])
                for element in h2_elements:
                    tempfile.write(f"{element.get_text()}\n" * 5)
                del h2_elements

                # write h3 text with weight 5
                h3_elements = soupified.find_all(['h3'])
                for element in h3_elements:
                    tempfile.write(f"{element.get_text()}\n" * 4)
                del h3_elements

                # write h4 text with weight 4
                h4_elements = soupified.find_all(['h4'])
                for element in h4_elements:
                    tempfile.write(f"{element.get_text()}\n" * 3)
                del h4_elements
                
                # write h5 text with weight 3
                h5_elements = soupified.find_all(['h5'])
                for element in h5_elements:
                    tempfile.write(f"{element.get_text()}\n" * 2)
                del h5_elements

                # # save outgoing links to a separate shelve
                # a_elements = soupified.find_all(['a'])
                # with shelve.open(f"outgoings/{OUTGOING_LINKS_SAVE_NAME}-{tid}") as outgoing:
                #     outgoing[str(idx)] = []
                #     for element in a_elements:
                #         if element.get('href') is not None:
                #             outgoing[str(idx)].append(element.get('href'))

                tempfile.write(soupified.get_text()) # write text content to temp file
            
            # tokenize tempfile
            tempfile_tokens = reader.tokenize(f"contents/{TEMP_FILE_NAME}-{tid}.txt")
            tempfile_tokens = [stemmer.stem(single_token) for single_token in tempfile_tokens]
            # function to iterate through tokens
                # enumerate token and index in list for position
                # returns dictionary mapping tokens to Posting objects
            tempfile_postings = reader.post_tokens(idx, tempfile_tokens)
            
            # add postings to fast postings variable
            for token in tempfile_postings:
                if token in postings:
                    postings[token].append(tempfile_postings[token])
                else:
                    postings[token] = [tempfile_postings[token]]
            
            # push fast postings variable data to shelve (disk) every 1000 files
            if not idx % 1000 or idx == end:
                with open(f"loggers/logger-{tid}.txt", "a") as log:
                    log.write(f"Beginning Offloading at {time.time()}\n")
                for token in postings:
                    if token in omega:
                        omega[token] += postings[token]
                    else:
                        omega[token] = postings[token]
                postings.clear()
                with open(f"loggers/logger-{tid}.txt", "a") as log:
                    log.write(f"Offloaded 1k files, time: {time.time()-start_time}\n")
            # end for loop filename
        # end for loop dirpath
    # end with open shelve
    os.remove(f"contents/{TEMP_FILE_NAME}-{tid}.txt")
    print(f"tid_{tid} ending")
                


def initalize_threads() -> None:
    """
    Initializes a group of threads to target a portion of processing the json files
    """
    thread_1 = threading.Thread(target=generate_inverted_index, args=(0,2499,1))
    thread_2 = threading.Thread(target=generate_inverted_index, args=(2500,4999,2))
    thread_3 = threading.Thread(target=generate_inverted_index, args=(5000,7499,3))
    thread_4 = threading.Thread(target=generate_inverted_index, args=(7500,9999,4))
    thread_5 = threading.Thread(target=generate_inverted_index, args=(10000,14999,5))
    thread_6 = threading.Thread(target=generate_inverted_index, args=(15000,19999,6))
    thread_7 = threading.Thread(target=generate_inverted_index, args=(20000,24999,7))
    thread_8 = threading.Thread(target=generate_inverted_index, args=(25000,29999,8))
    thread_9 = threading.Thread(target=generate_inverted_index, args=(30000,34999,9))
    thread_10 = threading.Thread(target=generate_inverted_index, args=(35000,39999,10))
    thread_11 = threading.Thread(target=generate_inverted_index, args=(40000,44999,11))
    thread_12 = threading.Thread(target=generate_inverted_index, args=(45000,49999,12))
    thread_13 = threading.Thread(target=generate_inverted_index, args=(50000,55392,13))

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()
    thread_6.start()
    thread_7.start()
    thread_8.start()
    thread_9.start()
    thread_10.start()
    thread_11.start()
    thread_12.start()
    thread_13.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()
    thread_6.join()
    thread_7.join()
    thread_8.join()
    thread_9.join()
    thread_10.join()
    thread_11.join()
    thread_12.join()
    thread_13.join()
        

if __name__ == '__main__':
    with open("id_to_file.txt", "r") as f:
        line = f.readline().strip()
        while line:
            docID, url = int(line.split()[0]), line.split()[1]
            ID_TO_FILE[docID] = url
            line = f.readline().strip()
    start_time = time.time()
    if os.path.exists("contents"): rmdir("contents")
    if os.path.exists("loggers"): rmdir("loggers")
    if os.path.exists("partials"): rmdir("partials")
    if os.path.exists("outgoings"): rmdir("outgoings")
    os.makedirs("contents")
    os.makedirs("loggers")
    os.makedirs("partials")
    os.makedirs("outgoings")
    initalize_threads()
    print("--- %s seconds ---" % (time.time() - start_time))

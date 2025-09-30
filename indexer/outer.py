import json
import indexer as i
import reader as r
import filterer as f
from bs4 import BeautifulSoup as soup
import os
from nltk.stem import PorterStemmer as PS
import threading
import shelve

FILE_LOCK = threading.Lock()

def process_files(start: int, end: int, tid: int) -> None:
    print(f'tid: {tid} | {start} to {end}')
    for idx in range(start, end+1):
        data = dict()
        filepath = i.ID_TO_FILE[idx]
        #print(f"processing {idx} / {filepath}")
        with open(filepath, "r") as json_file:
            data = json.load(json_file)
            soupified = soup(data['content'], features='lxml')
            a_tags = soupified.find_all("a")
            links = [a.get('href') for a in a_tags if a.get('href')]
            print(f"{idx} has {len(links)}")
            with FILE_LOCK:
                with shelve.open("OMEGA_OUTGOING", writeback=True) as omega:
                    omega[str(idx)] = links

            
def run():
    print(f"Starting threads...")
    thread_1 = threading.Thread(target=process_files, args=(0,3333,1))
    thread_1_1 = threading.Thread(target=process_files, args=(3334,6666,1_1))
    thread_1_2 = threading.Thread(target=process_files, args=(6667,9999,1_2))
    thread_2 = threading.Thread(target=process_files, args=(10000,19999,2))
    thread_3 = threading.Thread(target=process_files, args=(20000,29999,3))
    thread_4 = threading.Thread(target=process_files, args=(30000,39999,4))
    thread_5 = threading.Thread(target=process_files, args=(40000,49999,5))
    thread_6 = threading.Thread(target=process_files, args=(50000, 55392,6))
    thread_1.start()
    thread_1_1.start()
    thread_1_2.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()
    thread_6.start()
    thread_1.join()
    thread_1_1.join()
    thread_1_2.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()
    thread_6.join()
    print(f"All threads done!")


def id_files():
    print(f"Processing id to file paths...")
    with open("id_to_file.txt", "r") as f:
        line = f.readline().strip()
        while line:
            docID, url = int(line.split()[0]), line.split()[1]
            i.ID_TO_FILE[docID] = url
            line = f.readline().strip()
    print(f"Completed processing id to file paths!")

if __name__ == '__main__':
    if os.path.exists("OMEGA_OUTGOING"): os.remove("OMEGA_OUTGOING")
    id_files()
    run()
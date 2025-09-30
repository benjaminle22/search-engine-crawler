import shelve
from reader import normalize, absolutize
from time import sleep

def show_outgoing() -> None:
    with shelve.open("OMEGA_OUTGOING_ABSONORM") as omega:
        for key in omega.keys():
            print(f"{key}\n{omega[key]}\n")
            sleep(2)

def normalize_ID_URL() -> None:
    with shelve.open("ID_URL") as urls:
        with shelve.open("ID_URL_NORMAL") as normals:
            for doc_id in urls.keys():
                print(doc_id)
                normals[doc_id] = normalize(urls[doc_id])

def absolutize_outgoings() -> None:
    with shelve.open("OMEGA_OUTGOING") as omega:
        with shelve.open("OMEGA_OUTGOING_ABSONORM") as absonorm:
            with shelve.open("ID_URL_NORMAL") as root_urls:
                for doc_id in omega.keys():
                    print(doc_id)
                    links = omega[doc_id] # list of outgoing links in a document
                    new_links = [normalize(absolutize(root_urls[doc_id], link)) for link in links]
                    absonorm[doc_id] = new_links

def httpsize_shelves() -> None:
    with shelve.open("ID_URL_NORMAL") as normals:
        with shelve.open("ID_URL_NORMAL_HTTPS") as https:
            for doc_id in normals.keys():
                print(doc_id)
                https[doc_id] = normalize(normals[doc_id])
    with shelve.open("OMEGA_OUTGOING_ABSONORM") as absonorm:
        with shelve.open("OMEGA_OUTGOING_ABSONORM_HTTPS") as https:
            for doc_id in absonorm.keys():
                print(doc_id)
                https[doc_id] = [normalize(url) for url in absonorm[doc_id]]
                print(https[doc_id])

def count_outgoings() -> None:
    url_id = dict()
    with shelve.open("ID_URL_NORMAL_HTTPS") as urls:
        for doc_id in urls.keys():
            url_id[urls[doc_id]] = doc_id
            # print(f"{urls[doc_id]} mapped to {doc_id}")
    corpus_urls = set()
    with shelve.open("ID_URL_NORMAL_HTTPS") as urls:
        for doc_id in urls.keys():
            corpus_urls.add(urls[doc_id])
    with shelve.open("OMEGA_OUTGOING_ABSONORM_HTTPS") as absonorm:
        with shelve.open("ID_NUM_OUTGOINGS") as nums:
            for doc_id in absonorm.keys():
                count = 0
                links = absonorm[doc_id]
                for link in links:
                    if link in corpus_urls:
                        if doc_id != url_id[link]: count += 1
                print(f"{doc_id} has {count} from {len(links)}")
                nums[doc_id] = count

def write_outgoing_counts() -> None:
    with shelve.open("ID_NUM_OUTGOINGS") as nums:
        with open("ID_NUM_OUTGOINGS.txt", "w") as text:
            for doc_id in nums.keys():
                text.write(f"{doc_id} {nums[doc_id]}\n")
                print(f"{doc_id} {nums[doc_id]}")

def list_incomings() -> None:
    url_id = dict()
    with shelve.open("ID_URL_NORMAL_HTTPS") as urls:
        for doc_id in urls.keys():
            url_id[urls[doc_id]] = doc_id
            # print(f"{urls[doc_id]} mapped to {doc_id}")
    with shelve.open("ID_INCOMING_LIST", writeback=True) as incomings:
        with shelve.open("OMEGA_OUTGOING_ABSONORM_HTTPS") as omega:
            for doc_id in omega.keys():
                print(f"{doc_id}")
                for link in omega[doc_id]:
                    if link in url_id:
                        if doc_id != url_id[link]:
                            #print(f"{url_id[link]} aka {link} is in corpus!")
                            if url_id[link] in incomings:
                                incomings[url_id[link]].add(doc_id)
                                #print(f"{url_id[link]} adds {doc_id}")
                                #print(f"{url_id[link]} has {incomings[url_id[link]]}")
                            else:
                                incomings[url_id[link]] = {doc_id}
                                #print(f"{url_id[link]} created with {doc_id}")

def print_incomings() -> None:
    with shelve.open("ID_INCOMING_LIST") as incomings:
        for doc_id in incomings.keys():
            if len(incomings[doc_id]) > 1:
                print(f"{doc_id} referenced by {incomings[doc_id]}")

def write_incomings() -> None:
    with shelve.open("ID_INCOMING_LIST") as incomings:
        with open("ID_INCOMING_LIST.txt", "w") as text:
            for doc_id in incomings.keys():
                text.write(f"{doc_id}|{[int(elem) for elem in list(incomings[doc_id])]}\n")


if __name__ == '__main__':
    #show_outgoing()
    normalize_ID_URL()
    absolutize_outgoings()
    httpsize_shelves()
    count_outgoings()
    write_outgoing_counts()
    list_incomings()
    #print_incomings()
    write_incomings()


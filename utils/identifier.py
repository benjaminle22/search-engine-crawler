import os
import json
import shelve

def generate_shelve() -> None:
    if os.path.exists("ID_URL"):
        os.remove("ID_URL")
    count = 0
    with shelve.open("ID_URL") as id_url:
        for dirpath, dirnames, filenames in os.walk(f"./DEV/"):
            for filename in filenames:
                with open(dirpath + "/" + filename) as file:
                    print(f"Processing: {os.path.join(dirpath, filename)}, {count}")
                    data = json.load(file)
                    url = data["url"]
                    id_url[f"{count}"] = url
                    count += 1

def generate_shelve_txt() -> None:
    with shelve.open("ID_URL") as id_url:
        for key, val in id_url.items():
            with open("id-to-url.txt",'a') as f:
                f.write(f"{key} {val}\n")

if __name__ == '__main__':
    generate_shelve_txt()
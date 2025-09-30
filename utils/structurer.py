import os
from time import sleep

def analyze() -> None:
    subdomains = os.listdir(f"./DEV/")
    print(f"Subdomain count: {len(subdomains)}")
    count = 0
    print(f"Warning! Fast text wall incoming.")
    sleep(2)
    for dirpath, dirnames, filenames in os.walk(f"./DEV/"):
        print(f"Directory: {dirpath}")
        print(f"Subdirectories: {dirnames}")
        print(f"Files: {filenames}")
        count += len(filenames)
    print(f"Document count: {count}")
    


if __name__ == '__main__':
    analyze()
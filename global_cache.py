from typing import Dict, Set
from os.path import exists
from os import mkdir
import scraper as s
import shelve

def shelve_globals():
    """
    Store globals into a file to persist across different program instances
    """
    with shelve.open("gc_shelf") as shelf:
        shelf["page_count"] = s.page_count
        shelf["largest_word_count"] = s.largest_word_count
        shelf["words_frequencies"] = s.words_frequencies
        shelf["seen_domains"] = s.seen_domains
        shelf["forbidden_crawls"] = s.forbidden_crawls
        shelf["robots_data"] = s.robots_data
        shelf["ics_subdomains"] = s.ics_subdomains
        shelf["fingerprints"] = s.fingerprints

def unshelve_globals():
    """
    Loads previously saved globals onto current program instance
    """
    if exists("gc_shelf"):
        with shelve.open("gc_shelf") as shelf:
            s.page_count = shelf["page_count"]
            s.largest_word_count = shelf["largest_word_count"]
            s.words_frequencies = shelf["words_frequencies"]
            s.seen_domains = shelf["seen_domains"]
            s.forbidden_crawls = shelf["forbidden_crawls"]
            s.robots_data = shelf["robots_data"]
            s.ics_subdomains = shelf["ics_subdomains"]
            s.fingerprints = shelf["fingerprints"]


# EVERYTHING BELOW IS UNUSED.

def load_globals():
    if exists("gc"):
        s.page_count, s.largest_word_count = load_page_counts_and_largest_word_counts()
        s.words_frequencies = load_words_counts()
        s.seen_domains = load_domains_counts()
        s.forbidden_crawls = load_forbiddens()
        s.robots_data = load_robots()

def save_globals():
    if not exists("gc"):
        mkdir("gc")
    save_page_counts_and_largest_word_counts(s.page_count, s.largest_word_count)
    save_words_counts(s.words_frequencies)
    save_domains_counts(s.seen_domains)
    save_forbiddens(s.forbidden_crawls)
    save_robots(s.robots_data)

def save_page_counts_and_largest_word_counts(pages: int, words: int) -> None:
    """
    Given the current count of unique pages,
    and the current count of highest read words,
    save them into a file
    """
    with open("gc/pages_words_counts.txt", "w") as the_file:
        the_file.write(f"{pages} {words}")

def load_page_counts_and_largest_word_counts() -> tuple[int, int]:
    """
    Returns a tuple of unique page counts
    and highest read words counts previously saved into a file
    """
    group = (0, 0)
    if exists("gc/pages_words_counts.txt"):
        with open("gc/pages_words_counts.txt", "r") as the_file:
            line = the_file.read()
            line = line.split()
            group = (int(line[0]), int(line[1]))
    return group

def save_forbiddens(forbiddens: Set[str]) -> None:
    """
    Given a set of forbidden urls (str), save them into a text file
    """
    with open("gc/forbiddens.txt", "w") as forbiddens_txt:
        for url in forbiddens:
            forbiddens_txt.write(f"{url}\n")

def load_forbiddens() -> Set[str]:
    """
    Returns a set of forbidden urls previously saved in text file
    """
    forbiddens = set()
    if exists("gc/forbiddens.txt"):
        with open("gc/forbiddens.txt", "r") as forbiddens_txt:
            for line in forbiddens_txt:
                url = line.strip()
                if url != "": forbiddens.add(url)
    return forbiddens

def save_robots(robots: Dict[str, str]) -> None:
    """
    Given a dictionary mapping a domain (str) to its robots.txt content (str),
    save them into a file in a format to be loaded back in later
    """
    with open("gc/mega_robots.txt", "w") as mega_robots:
        for domain, robots_text in robots.items():
            mega_robots.write(f"{domain}==={robots_text}```")

def load_robots() -> Dict[str, str]:
    """
    Returns a dictionary mapping a domain (str) to its robots.txt content (str)
    previously saved into a text file
    """
    robots = dict()
    if exists("gc/mega_robots.txt"):
        with open("gc/mega_robots.txt", "r") as mega_robots:
            text = mega_robots.read()
            text = text.split("```")
            for entry in text:
                domain, robots_text = entry.split("===")
                robots[domain] = robots_text
    return robots

def save_words_counts(words_counts: Dict[str, int]) -> None:
    """
    Given a dictionary mapping a word (str) to its frequency count (int),
    save them into a csv to be loaded back in later
    """
    with open("gc/words_counts.csv", "w") as words_counts_csv:
        for word, count in words_counts.items():
            words_counts_csv.write(f"{word},{count}\n")

def load_words_counts() -> Dict[str, int]:
    """
    Returns a dictionary mapping a word (str) to its frequency count (int)
    previously saved in text file
    """
    words_counts = dict()
    if exists("gc/words_counts.csv"):
        with open("gc/words_counts.csv", "r") as words_counts_csv:
            for line in words_counts_csv:
                word, count = line.strip().split(",")
                words_counts[word] = int(count)
    return words_counts

def save_domains_counts(domains_counts: Dict[str, int]) -> None:
    """
    Given a dictionary mapping a domain (str) to its associated page counts (int),
    save them into a csv to be loaded back in later
    """
    with open("gc/domains_page_counts.csv", "w") as domains_page_counts_csv:
        for domain, page_counts in domains_counts.items():
            domains_page_counts_csv.write(f"{domain},{page_counts}\n")

def load_domains_counts() -> Dict[str, int]:
    """
    Returns a dictionary mapping a domain (str) to its associated page counts (int)
    previously saved in a csv
    """
    domain_page_counts = dict()
    if exists("gc/domains_page_counts.csv"):
        with open("gc/domains_page_counts.csv", "r") as domains_page_counts_csv:
            for line in domains_page_counts_csv:
                domain, page_count = line.strip().split(",")
                domain_page_counts[domain] = int(page_count)
    return domain_page_counts

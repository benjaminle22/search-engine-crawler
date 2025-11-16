import os
import shelve
import re

from threading import Thread, RLock, Lock
from queue import Queue, Empty
from urllib.parse import urlparse
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid, forbidden_crawls

ics_lock = Lock()
cs_lock = Lock()
informatics_lock = Lock()
stat_lock = Lock()

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        # 0:ics, 1:cs, 2:informatics, 3:stat
        self.to_be_downloaded = {0:Queue(), 1:Queue(), 2:Queue(), 3:Queue()}
        self.seen_domains = set()
        self.skipped_urls = set()
         
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def determine_domain(self, url: str) -> int:
        """
        Return an integer associated with the domain
        
        0 : ics.uci.edu
        1 : cs.uci.edu
        2 : informatics.uci.edu
        3 : stat.uci.edu
        4 : invalid domain
        """
        if re.match(r".*([^a-zA-Z0-9])ics\.uci\.edu.*", url): return 0
        elif re.match(r".*([^a-zA-Z0-9])cs\.uci\.edu.*", url): return 1
        elif re.match(r".*([^a-zA-Z0-9])informatics\.uci\.edu.*", url): return 2
        elif re.match(r".*([^a-zA-Z0-9])stat\.uci\.edu.*", url): return 3
        else: return 4

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            domain_int = self.determine_domain(url)
            if not completed and is_valid(url):
                match domain_int:    
                    case 0:
                        self.to_be_downloaded[domain_int].put(url)
                    case 1:
                        self.to_be_downloaded[domain_int].put(url)
                    case 2:
                        self.to_be_downloaded[domain_int].put(url)
                    case 3:
                        self.to_be_downloaded[domain_int].put(url)
                    case 4:
                        pass
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self, domain_int: int):
        # domain_int is worker_id of thread
        try:
            match domain_int:    
                case 0:
                        return self.to_be_downloaded[domain_int].get()
                case 1:
                        return self.to_be_downloaded[domain_int].get()
                case 2:
                        return self.to_be_downloaded[domain_int].get()
                case 3:
                        return self.to_be_downloaded[domain_int].get()
                case 4:
                    return None
        except IndexError:
            return None

    def write_tbd(self, domain_int: int) -> None:
        if not (0 <= domain_int <= 3): return
        with open(f"tbd{domain_int}.txt", "a+") as tbd_file:
            tbd_file.write(f"{self.to_be_downloaded[domain_int].qsize()}\n")

    def add_url(self, url):
        self.write_tbd(self.determine_domain(url))
        domain = urlparse(url)._replace(path='', params='', query='', fragment='').geturl()
        domain_int = self.determine_domain(url)
        if domain not in self.seen_domains:
            # if we haven't seen this domain before, we should get the robots.txt file from it first (if it exists)
            self.seen_domains.add(domain) # add domain to seen_domains
            self.save[get_urlhash(normalize(domain + '/robots.txt'))] = (normalize(domain + '/robots.txt'), False)
            self.save.sync()
            self.save[get_urlhash(normalize(domain))] = (normalize(domain), False)
            self.save.sync()
            match domain_int:
                case 0:
                    with ics_lock:
                        self.to_be_downloaded[domain_int].put(url)
                        self.to_be_downloaded[domain_int].put(normalize(domain + '/robots.txt'))
                case 1:
                    with cs_lock:
                        self.to_be_downloaded[domain_int].put(url)
                        self.to_be_downloaded[domain_int].put(normalize(domain + '/robots.txt'))
                case 2:
                    with informatics_lock:
                        self.to_be_downloaded[domain_int].put(url)
                        self.to_be_downloaded[domain_int].put(normalize(domain + '/robots.txt'))
                case 3:
                    with stat_lock:
                        self.to_be_downloaded[domain_int].put(url)
                        self.to_be_downloaded[domain_int].put(normalize(domain + '/robots.txt'))
                case 4:
                    pass
            return 
        if domain in forbidden_crawls: return # if the entire domain has been barred from crawling, don't bother.
        url = normalize(url)
        if url in forbidden_crawls: return # if this specifc path is blacklisted, don't bother
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            match domain_int:    
                case 0:
                    with ics_lock:
                        self.to_be_downloaded[domain_int].put(url)
                case 1:
                    with cs_lock:
                        self.to_be_downloaded[domain_int].put(url)
                case 2:
                    with informatics_lock:
                        self.to_be_downloaded[domain_int].put(url)
                case 3:
                    with stat_lock:
                        self.to_be_downloaded[domain_int].put(url)
                case 4:
                    pass          
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")
        self.save[urlhash] = (url, True)
        self.save.sync()

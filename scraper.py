import re
from crawler import frontier
from urllib.parse import urlparse, urljoin, urldefrag
from urllib.robotparser import RobotFileParser as RFP
import ssl
from bs4 import BeautifulSoup
from utils import normalize
from assignment1 import tokenize, compute_word_frequencies
from report import stopwords
from os import remove
from simhash import *
from threading import Thread, Lock
from assignment1 import is_alnum

page_count_lock = Lock()
largest_word_count_lock = Lock()
words_frequencies_lock = Lock()
seen_domains_lock = Lock()
forbidden_crawls_lock = Lock()
robots_data_lock = Lock()
ics_subdomains_lock = Lock()
fingerprints_lock = Lock()

page_count = 0 # variable tracking number of seen unique pages
largest_word_count = 0 # variable tracking the highest word count seen in a page
words_frequencies = dict() # dictionary mapping words to their frequencies across all pages
seen_domains = set() # a set that holds domains seen after reading its robots.txt
forbidden_crawls = set() # a set to hold all links whihc have been listed as disallow in robots.txt
robots_data = dict() # a dict mapping a domain to its robots.txt text content
ics_subdomains = dict() # a dict mapping an ics sub domain to its unique page count
fingerprints = dict() # a dict that maps all fingerprints of downloaded webpages to their urls

def scraper(url, resp, worker_id):
    #TODO: sets to keep track of domains
    #TODO: inspect the domain's robots.txt (if not seen before) and verify are we actually allowed to crawl?
    # also grab sitemaps while at it
    '''
    procedure CrawlerThread(frontier)
        while not frontier.done() do
            website ← frontier.nextSite()
            url ← website.nextURL()
            if website.permitsCrawl(url) then
                text ← retrieveURL(url)
                storeDocument(url, text)
                for each url in parse(text) do
                    frontier.addURL(url)
                end for
            end if
                frontier.releaseSite(website)
        end while
    end procedure
    '''
    # return empty list if response status is unsatisfactory
    if resp.status == 204 or 600 <= resp.status <= 606 or resp.status == 404:
        with open("error.txt", "a") as f:
            f.write(f"{resp.url} has status {resp.status}\n")
        return list() # nothing to return because of error
    # TODO: Use BeautifulSoup for politeness + robots.txt
    # TODO: inserts inspecttion of the robots.txt here

    # parse the content
    links = extract_next_links(url, resp, worker_id)
    return [link for link in links if is_valid(link)]

def _can_we_crawl_here_domain(resp) -> list[str] | None:
    """Checks a URL's domain to see if 
    crawling through it is allowed or not, given a resp object
    of that domain's robots.txt file.
    If crawling is permitted, returns a list of links to add to the queue
    (home page + sitemaps).
    If not, returns None, and blacklists the domain from future crawls.
    """
    
    # Parse URL
    domain = urlparse(resp.url)._replace(path='', params='', query='', fragment='')

    # Initialize robot file parser
    robots_parser = RFP()
    robots_parser.set_url(domain.geturl() + '/robots.txt')

    # Set the link to the domain's robots.txt
    robots_parser.parse(resp.raw_response.content.decode(errors='ignore').splitlines()) # feeds the robots.txt to the parser

    # Save the robots.txt content for the domain in global robots_data
    # TODO: what if there is no robots.txt
    with robots_data_lock:
        robots_data[domain.geturl()] = resp.raw_response.content.decode(errors='ignore').splitlines()

    if not robots_parser.can_fetch('*', domain.geturl()): # is this domain forbidden by robots.txt?
        with forbidden_crawls_lock:
            forbidden_crawls.add(domain.geturl()) # add to global var tracking forbidden urls
        with open("error.txt", "a") as f:
            f.write(f"{resp.url} is banned from crawling\n")
        return None
    res = [domain.geturl()]
    sitemaps = robots_parser.site_maps() 
    if sitemaps is not None:
        res.extend(sitemaps)
    else:
        res.append(domain.geturl() + '/sitemap.xml')
        res.append(domain.geturl() + '/sitemapindex.xml')
        res.append(domain.geturl() + '/sitemap_index.xml')
        res.append(domain.geturl() + '/sitemap-index.xml')
        res.append(domain.geturl() + '/sitemap.xml')
        res.append(domain.geturl() + '/post-sitemap')
        res.append(domain.geturl() + '/page-sitemap')
    
    # adds domain to dictionary of seen domains
    with seen_domains_lock:
        seen_domains.add(domain.geturl())
    return res

def _is_url_banned(resp) -> bool:
    """checks if a specifc url is cleared for crawling, based
    off its domain's robots.txt content"""
    domain = urlparse(resp.url)._replace(path='', params='', query='', fragment='')
    robots_parser = RFP()
    robots_parser.set_url(domain.geturl() + '/robots.txt')
    # sets the parser to the domain's robots.txt file
    with robots_data_lock:
        robots_parser.parse(robots_data[domain.geturl()]) # feeds the robots.txt of the domain to the parser
    return robots_parser.can_fetch('*', normalize(resp.url)) # is this url forbidden by its domain's robots.txt?

def _is_domain_checked(resp) -> bool:
    """A preliminary function that verifies that the url's domain has been
    properly vetted for crawling permissions (i.e. its robots.txt page has been
    checked)"""
    domain = urlparse(resp.url)._replace(path='', params='', query='', fragment='')
    with seen_domains_lock:
        return domain.geturl() in seen_domains # only true once its robots.txt has been searched

def extract_next_links(url, resp, worker_id):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # List to hold all valid links
    valid_links = list()

    # Return empty list if raw_response is None
    if not resp.raw_response:
        return list()
    
    # Soup up raw response
    soup = BeautifulSoup(resp.raw_response.content, 'lxml')

    # Import globals for use
    global largest_word_count, page_count

    # Mutex for incrementing unique page count
    with page_count_lock:
        page_count += 1

    # TODO: store this?
    # TODO: similarity detetcion / trap check with RegEx (scheme + netcode + path)
    # TODO: check robots.txt + politeness 
    # NOTE: sitemap is in robots.txt!

    # if url endswith robots.txt: parse it + get links from sitemaps in loc tags
    if url.endswith('/robots.txt'): # this is the domains's robots.txt (should come first)
        robot_check = _can_we_crawl_here_domain(resp)
        if robot_check is None: return list()# crawling for this URL is forbidden if this is true
        else:
            valid_links.extend(robot_check) # adds homepage + sitemaps links to valid_links
    else: # this is NOT the domain's robots.txt (should come after we process its robots.txt)
        if not _is_domain_checked(resp): return [url] # returns the url, as we haven't checked this domain yet
        if not _is_url_banned(resp): # is this url cleared for crawling, according to its domain's robots.txt?
            with forbidden_crawls_lock:
                forbidden_crawls.add(resp.raw_response.url) # add to global var in frontir.py
            with open("error.txt", "a") as f:
                f.write(f"{resp.url} is banned from crawling\n")
            return list()
        # otheriwse, we're clear for crawling.
    for sm_url in soup.find_all('loc'): # gets all links hidden in loc tags in HTML (for sitemaps)
        if sm_url.text not in {'#', '/'}: #does the loc url even exist, and if so, is it to a new link?
            # # and / indicate the same page
            url_scheme = urlparse(urldefrag(sm_url.text).url)._replace(query="")
            # breaks a (defragmented) url into components to see if it's a relative or absolute
            if not url_scheme.scheme or not url_scheme.netloc: # if scheme and netloc is None, it's a relative URL
                url_result = urljoin(url, url_scheme.geturl())
            else:
                # otherwise, it is already absolute
                url_result = url_scheme.geturl()
            valid_links.append(url_result)
    for i in soup.find_all('a'): # gets all links hidden in anchor tags in HTML
        href_url = i.get('href')
        if href_url and href_url not in {'#', '/'}: #does the href url even exist, and if so, is it to a new link?
            # # and / indicate the same page
            url_scheme = urlparse(urldefrag(href_url).url)._replace(query="")
            # breaks a (defragmented) url into components to see if it's a relative or absolute
            if not url_scheme.scheme or not url_scheme.netloc: # if scheme and netloc is None, it's a relative URL
                url_result = urljoin(url, url_scheme.geturl())
            else:
                # otherwise, it is already absolute
                url_result = url_scheme.geturl()
            valid_links.append(url_result)
    
    # Store raw response into html.parser format for tokenizing
    textsoup = BeautifulSoup(resp.raw_response.content, 'html.parser')
    # Write raw content into temp file
    with open(f"soup_text{worker_id}.txt", "w+") as soup_text_txt:
        soup_text_txt.write(textsoup.get_text())
    # Get tokens from temp file
    tokens = tokenize(f"soup_text{worker_id}.txt")
    # Update largest word count if this is the biggest page so far
    with largest_word_count_lock:
        if len(tokens) > largest_word_count: largest_word_count = len(tokens)
    # Remove stopwords
        # filter takes in a function and an iterable, returning an iterator
    tokens = list(filter(is_relevent_word, tokens))

    # Get the word frequencies in a dictionary
    frequencies = compute_word_frequencies(tokens)

    # Fingerprint the web content
    fingerprint = get_fingerprint(frequencies)
    # Return empty list if fingerprint is similar to saved fingerprints
    if not resp.raw_response.url.endswith('/robots.txt') and not resp.raw_response.url.endswith('.xml'):
        # Mutex for fingerprint set access
        with fingerprints_lock:
            matched_url = check_similarities(fingerprints, fingerprint)
            if matched_url != "":
                fingerprints[fingerprint] = resp.raw_response.url
                with open("error.txt", "a") as f:
                    f.write(f"{resp.url} is too similar to {matched_url}\n")
                return list()
            else:
                fingerprints[fingerprint] = resp.raw_response.url

    # Add the word frequencies to the general word frequency tracker across all pages
    if not resp.raw_response.url.endswith('/robots.txt') and not resp.raw_response.url.endswith('.xml'):
        # Mutex for general dictionary access
        with words_frequencies_lock:
            for word in frequencies:
                if word in words_frequencies: # if word already exists
                    words_frequencies[word] += frequencies[word] # add to count
                else: # if word doesn't exist
                    words_frequencies[word] = frequencies[word] # create entry

    # If url is an ics subdomain, then record it into dictionary mapping ics subdomains to its page counts
    if re.match(r".*([^a-zA-Z0-9])ics\.uci\.edu.*", resp.raw_response.url):
        subdomain = get_ics_subdomain(resp.raw_response.url)
        with ics_subdomains_lock:
            if subdomain in ics_subdomains: ics_subdomains[subdomain] += 1
            else: ics_subdomains[subdomain] = 1    
    return valid_links

def get_ics_subdomain(url: str) -> str:
    parsed_url = urlparse(url)
    domains = parsed_url.hostname.split(".")
    if len(domains) > 2:
        return ".".join(domains)
    else:
        return ""

def is_relevent_word(word: str) -> bool:
    """
    Returns bool dependent on whether word is in set of stopwords
    """
    word = word.strip()
    alnum = True
    for char in word:
        alnum = alnum and is_alnum(char)
    return word not in stopwords and len(word) > 1 and alnum

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    # regEx: .*.ics.uci.edu\/.*
    '''
    conditions for being invalid:
    - not a valid domain
    '''
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not re.match(r"(.*([^a-zA-Z0-9])*ics\.uci\.edu.*)|(.*([^a-zA-Z0-9])*cs\.uci\.edu.*)|(.*([^a-zA-Z0-9])*informatics\.uci\.edu.*)|(.*([^a-zA-Z0-9])*stat\.uci\.edu.*)", parsed.netloc.lower()):
            '''is this url a member of one of the four valid domains?'''
            return False
        if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", parsed.path.lower()):
            '''The above regex is from https://support.archive-it.org/hc/en-us/articles/208332963-Modify-crawl-scope-with-a-Regular-Expression#InvalidURLs'''
            """does this string contain a repeating directory?"""
            return False
        if re.match(r".*\/page\/.*", parsed.path.lower()):
            """is this a page URL (i.e.) https://www.cs.uci.edu/news/page/2 ?"""
            """there were a great deal of these pages, many of which linked to content which could be found via sitemaps instead"""
            return False
        # TODO: regexs for .*/[a-zA-Z0-9]\d\..* and calendar URLs
        if re.match(r".*\/[1-9][0-9][0-9]{2}-([0][1-9]|[1][0-2])-([1-2][0-9]|[0][1-9]|[3][0-1]).*", parsed.path.lower()):
            """regex from https://regex101.com/library/NXvFly?orderBy=RELEVANCE&search=date&filterFlavors=javascript"""
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico|apk|img"
            + r"|png|tiff?|mid|mp2|mp3|mp4|war|jpg|png"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|mpg"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1|ppsx"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

#!/usr/bin/env python3

# A simple Web Crawler that takes a single URL as input and produces
# output of each URL and the links on that page.  It uses threads to
# speed processing.
#
# Note: Code does not use the the implicit GIL for serialization

import argparse
import threading
import urllib.request

from bs4 import BeautifulSoup
from typing import List
from urllib.parse import urlparse

g_urls_to_be_processed = []
g_urls_already_processed = []
g_num_threads_processing = 0
g_all_done = False

# Synchronization variables
g_url_condition_lock = threading.Lock()
g_url_condition = threading.Condition(g_url_condition_lock)
g_print_lock = threading.Semaphore()


def get_links_from_page_text(page_url: str, page_text: str) -> List[str]:
    """ Get the links from the page text
    Use the BeautifulSoup library to parse out the 'a' tags on a page.
    Return a list of links on the page.
    Args:
        page_url (str): URL of the page
        page_text (str): text of page
    Return:
        list of links on the page
    """

    links = []

    parsed_url = urlparse(page_url)
    pathless_url = parsed_url.scheme + "://" + parsed_url.netloc

    bs_page = BeautifulSoup(page_text, features="lxml")
    for link in bs_page.findAll('a'):
        path_or_url = (link.get('href'))
        if path_or_url is None or len(path_or_url) == 0:
            continue
        if path_or_url[0] == '/':
            # Relative links
            link_url = pathless_url + path_or_url
        elif path_or_url[0:3] == "http":
            # Absolute links
            link_url = path_or_url
        else:
            continue
        links.append(link_url)
    return links


def get_and_process_url() -> None:
    """ Fetch URL, parse out links, add links to those to be processed, and print
        This function uses a condition variable to synchronize access to the
        queue of urls to be processed.  If the queue is empty, threads wait
        on the condition variable.  When a thread is done processing a page,
        it does a notify on the condition variable if the queue is not empty.

        It also uses a semaphore to serialize printing to prevent interleaved
        output.
    """
    global g_num_threads_processing, g_all_done

    while True:
        g_url_condition.acquire()
        while True:
            # g_url_condition is held by this thread
            if len(g_urls_to_be_processed) > 0:
                page_url = g_urls_to_be_processed.pop(0)
                g_urls_already_processed.append(page_url)
                g_url_condition.release()
                g_num_threads_processing += 1
                break
            elif g_all_done:
                g_url_condition.release()
                return
            elif g_num_threads_processing == 0:
                g_all_done = True
                g_url_condition.notify_all()
                g_url_condition.release()
                return
            else:
                g_url_condition.wait()
                pass

        req = urllib.request.Request(page_url)
        try:
            with urllib.request.urlopen(req) as response:
                page_bytes = response.read()
                page_text = page_bytes.decode('utf8')
                page_links = get_links_from_page_text(page_url, page_text)

                g_url_condition.acquire()
                for link in page_links:
                    if link not in g_urls_already_processed and link not in g_urls_to_be_processed:
                        g_urls_to_be_processed.append(link)

                if len(g_urls_to_be_processed) > 0:
                    g_url_condition.notify_all()

                g_num_threads_processing -= 1
                g_url_condition.release()

                g_print_lock.acquire()
                print(page_url)
                for link in page_links:
                    print("  {}".format(link))
                g_print_lock.release()
        except Exception as e:
            # Not generally good to catch all errors, but doing it
            # here to shorten develop time.
            # Some of the exceptions that can happen:
            #   urllib.error.HTTPError, urllib.error.URLError
            #   UnicodeEncodeError, UnicodeDecodeError
            g_url_condition.acquire()
            g_num_threads_processing -= 1
            g_url_condition.release()



def main():
    num_threads = 1
    threads = []
    parser = argparse.ArgumentParser(description = "A simple Web Crawler")
    parser.add_argument("starting_url", help="Starting URL")
    parser.add_argument("--num_threads", "-t", help="number of threads to use")

    args = parser.parse_args()
    starting_url = args.starting_url
    if args.num_threads is not None:
        num_threads = int(args.num_threads)

    g_urls_to_be_processed.append(starting_url)

    for i in range(num_threads):
        threads.append(threading.Thread(target=get_and_process_url,
                                        name="Thread {}".format(i)))

    for i in range(num_threads):
        threads[i].start()

    for i in range(num_threads):
        threads[i].join()


if __name__ == "__main__":
    main()

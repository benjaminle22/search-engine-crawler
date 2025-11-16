from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from time import sleep

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            # Create four worker threads
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        while True:
            sleep(10)
            if not self.frontier.to_be_downloaded[0].qsize() \
                and not self.frontier.to_be_downloaded[1].qsize() \
                and not self.frontier.to_be_downloaded[2].qsize() \
                and not self.frontier.to_be_downloaded[3].qsize():
                print("ALL QUEUES EMPTY, TERMINATING.")
                raise KeyboardInterrupt
        self.join()
        

    def join(self):
        for worker in self.workers:
            worker.join()

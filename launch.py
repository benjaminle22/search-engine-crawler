from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
import global_cache as gc
import report as r


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    gc.unshelve_globals()
    try:
        crawler.start()
    except (KeyboardInterrupt, ConnectionError):
        gc.shelve_globals()
    except Exception as e:
        with open("bad_error.txt", "w") as bad_error_file:
            bad_error_file.write(f"Something bad happened :(\n\n{e}")
    r.write_report()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)


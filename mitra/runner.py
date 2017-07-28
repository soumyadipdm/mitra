"""
Mitra runner: a long running process that indexes list of files specified
"""

import os
import sys
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler

import yaml
#import daemon

from mitra.indexer import Indexer


class Config(object):
    """Configuration object"""

    def __init__(self, configfile):
        with open(configfile) as conf_file:
            self._config = yaml.load(conf_file)

        if not self._config.get("files"):
            raise ValueError(
                "No files to be indexed, please specify list of files in the in config file")

        if not self._config.get("frquency"):
            self._config["frequency"] = 300  # in seconds

        log_levels = {"info": logging.INFO, "debug": logging.DEBUG}
        log_defaults = {
            "file": "log/runner.log",
            "maxsize": 10,
            "level": log_levels["info"]}

        self._setdefaults(log_defaults, "log")

        if self._config["log"]["level"] not in log_levels:
            self._config["log"]["level"] = log_levels["info"]

        else:
            level = self._config["log"]["level"]
            self._config["log"]["level"] = log_levels[level]

        indexer_defaults = {
            "prefix": "mitra_",
            "es_host": "localhost",
            "es_port": 9200,
            "maxsize": 10}

        self._setdefaults(indexer_defaults, "indexer")

    def _setdefaults(self, defaults, config_param):
        if not self._config.get(config_param):
            self._config[config_param] = defaults

        for key in defaults:
            if not self._config[config_param].get(key):
                self._config[config_param] = {}
                self._config[config_param][key] = defaults[key]

    def __getattr__(self, attr):
        if attr not in self._config:
            raise AttributeError("{0} not found".format(attr))
        return self._config[attr]


def setup_logging(logfile, maxsize):
    """Setup logging to the specified log file

    :param logfile: string, path to the logfile
    :param maxsize: int, maximum log file size in MB
    """
    log = logging.getLogger()
    log.addHandler(logging.StreamHandler(sys.stdout))
    file_handler = RotatingFileHandler(
        logfile, maxBytes=(
            maxsize * 1024 ** 2), backupCount=10)
    log.addHandler(file_handler)
    log.setLevel(logging.INFO)


def main():
    """Main function"""
    basedir = os.path.abspath(os.path.join(os.path.basename(__file__), "../"))
    parser = argparse.ArgumentParser(description="Mitra runner daemon")
    parser.add_argument(
        "--config",
        "-c",
        help="Path to the YAML config file",
        default=os.path.join(
            basedir,
            "config/runner.yaml"))
    args = parser.parse_args()

    config = Config(args.config)
    setup_logging(config.log["file"], config.log["maxsize"])
    log = logging.getLogger()
    log.info("Starting up Mitra Runner")

    indexer = Indexer(
        config.indexer["prefix"],
        config.indexer["es_host"],
        config.indexer["es_port"],
        config.indexer["maxsize"])

    # with daemon.DaemonContext():
    try:
        while True:
            result = indexer.indexify(config.files)
            log.debug("Result: {0}".format(str(result)))
            log.info("Sleeping for {0}s".format(config.frequency))
            time.sleep(config.frequency)
    except KeyboardInterrupt:
        log.info("Stopping Mitra runner")


if __name__ == "__main__":
    main()

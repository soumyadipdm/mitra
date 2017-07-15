#!/usr/bin/env python2

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

        if not self._config.get("log"):
            self._config["log"] = {}
            self._config["log"]["file"] = "log/runner.log"
            self._config["log"]["maxsize"] = 10

        defaults = {
            "prefix": "mitra_",
            "es_host": "localhost",
            "es_port": 9200,
            "maxsize": 10}
        if not self._config.get("indexer"):
            self._config["indexer"] = defaults

        for key in defaults:
            if not self._config["indexer"].get(key):
                self._config["indexer"][key] = defaults[key]

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

    #with daemon.DaemonContext():
    while True:
        indexer.indexify(config.files)
        log.info("Sleeping for {0}s".format(config.frequency))
        time.sleep(config.frequency)

if __name__ == "__main__":
    main()

import os
import socket
import hashlib
import logging
from datetime import datetime

from elasticsearch import Elasticsearch


log = logging.getLogger()


def _doc_id(hostname, filename):
    """Generate a unique document ID from given hostname and filename

    :param hostname: string, preferrably fqdn of the host
    :param filename: string, path to the file
    :returns: string, id of the document
    """
    hash_ = hashlib.sha256()
    hash_.update("{0}_{1}".format(hostname, filename))
    return hash_.hexdigest()


class Indexer(object):
    """Indexer class"""

    def __init__(
            self,
            index_prefix="mitra_",
            es_host="localhost",
            es_port=9200,
            max_filesize=10):
        """
        :param index_prefix: string, prefix of the index to be used, default `mitra_`
        :param es_host: string, Elasticsearch instance hostname
        :param es_port: int, Elasticsearch instance port number
        :param max_filesize: int, maximum size of the files to be indexed, in MB
        """
        self._es = Elasticsearch(["{0}:{1}".format(es_host, es_port)])
        self.index_prefix = index_prefix
        self.max_filesize = max_filesize
        self.hostname = socket.gethostname()

    def _file_to_data(self, filename):
        """Reads a file and prepares data to index it in ES
        :param filename: string, path to the file
        :returns: None if file is not going to be indexed, or
        a dict containing fields to index the data into ES
        """
        if not os.path.isfile(filename):
            log.error("{0} is not a file or does not exist".format(filename))
            return

        # for safety reason we're not going to read a file
        # more than `max_filesize`
        if os.stat(filename).st_size > (self.max_filesize * 1024 * 1024):
            log.error(
                "{0} is more than 10MB, not going to index it".format(filename))
            return

        with open(filename) as file_:
            content = file_.read()

        data = {}
        data["filename"] = filename
        data["hostname"] = self.hostname
        data["timestamp"] = datetime.now()
        data["content"] = content

        return data

    def _to_index(self, data, filename):
        """Index the data into Elasticsearch

        :param data: dict, containing data to be indexed
        :param filename: string, path to the file
        """
        # we are re-indexing everyday
        today = datetime.today().strftime('%Y-%m-%d')
        index = "{0}{1}".format(self.index_prefix, today)
        result = self._es.index(
            index=index,
            doc_type="text",
            id=_doc_id(
                self.hostname,
                filename),
            body=data)
        log.debug(str(result))
        return bool(result["_shards"]["successful"])

    def indexify(self, files):
        """Push files to ES index

        :param files: list, a list of path to the files
        :returns: a dict containing success/failure per file
        """
        result = {}
        for file_ in files:
            data = self._file_to_data(file_)
            if not data:
                log.debug("No data could be generated for {0}".format(file_))
                continue
            is_success = self._to_index(data, file_)
            if not is_success:
                log.debug("Unable to index {0} into ES".format(file_))
            else:
                log.debug("Successfully indexed {0}".format(file_))
            result[file_] = is_success
        return result

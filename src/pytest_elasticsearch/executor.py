"""Elasticsearch executor."""

import base64
import re
from subprocess import check_output

from mirakuru import HTTPExecutor
from pkg_resources import parse_version


class NoopElasticsearch:  # pylint:disable=too-few-public-methods
    """No operation Elasticsearch executor mock."""

    def __init__(self, host, port, http_auth=None):
        """
        Initialize Elasticsearch executor mock
        :param str host: hostname under which elasticsearch is available
        :param int port: port under which elasticsearch is available
        """
        self.host = host
        self.port = port

        if http_auth is not None:
            self.login = http_auth['login']
            self.password = http_auth['password']

    @staticmethod
    def running():
        """Mock method pretending the executor is running."""
        return True


# pylint:disable=too-many-instance-attributes
class ElasticSearchExecutor(HTTPExecutor):
    """Elasticsearch executor."""

    def __init__(
        self,
        executable,
        host,
        port,
        tcp_port,
        pidfile,
        logs_path,
        works_path,
        cluster_name,
        network_publish_host,
        index_store_type,
        timeout,
        http_auth,
    ):  # pylint:disable=too-many-arguments
        """
        Initialize ElasticSearchExecutor.

        :param pathlib.Path executable: Executable path
        :param str host: hostname under which elasticsearch will be running
        :param int port: port elasticsearch listens on
        :param int tcp_port: port used for unternal communication
        :param pathlib.Path pidfile: pidfile location
        :param pathlib.Path logs_path: log files location
        :param pathlib.Path works_path: workdir location
        :param str cluster_name: cluster name
        :param str network_publish_host: network host to which elasticsearch
            publish to connect to cluseter'
        :param str index_store_type: type of the index to use in the
            elasticsearch process fixture
        :param int timeout: Time after which to give up to start elasticsearch
        :param dict http_auth: credentials (e.g. {'login': 'elastic', 'password': 'elastic'})
        """
        self._version = None
        self.executable = executable
        self.host = host
        self.port = port
        self.tcp_port = tcp_port
        self.pidfile = pidfile
        self.logs_path = logs_path
        self.works_path = works_path
        self.cluster_name = cluster_name
        self.network_publish_host = network_publish_host
        self.index_store_type = index_store_type
        self.http_auth = http_auth

        if self.http_auth is not None:
            login = self.http_auth['login']
            password = self.http_auth['password']
            token = base64.b64encode(f'{login}:{password}'.encode())
            headers = {'Authorization': f'Basic {token}'}
        else:
            headers = None

        super().__init__(
            self._exec_command(),
            f"http://{self.host}:{self.port}",
            timeout=timeout,
            headers=headers,
        )

    @property
    def version(self):
        """
        Get the given elasticsearch executable version parts.

        :return: Elasticsearch version
        :rtype: pkg_resources.Version
        """
        if not self._version:
            try:
                output = check_output([self.executable, "-Vv"]).decode("utf-8")
                match = re.search(r"Version: (?P<major>\d)\.(?P<minor>\d+)\.(?P<patch>\d+)", output)
                if not match:
                    raise RuntimeError(
                        "Elasticsearch version is not recognized. "
                        "It is probably not supported. \n"
                        "Output is: " + output
                    )
                version = match.groupdict()
                self._version = parse_version(
                    ".".join([version["major"], version["minor"], version["patch"]])
                )
            except OSError as exc:
                raise RuntimeError(
                    "'%s' does not point to elasticsearch." % self.executable
                ) from exc
        return self._version

    def _exec_command(self):
        """
        Get command to run elasticsearch binary based on the version.

        :return: command to run elasticsearch
        :rtype: str
        """
        if self.version < parse_version("6.0.0"):
            raise RuntimeError("This elasticsearch version is not supported.")
        return f"""
            {self.executable} -p {self.pidfile}
            -E http.port={self.port}
            -E transport.tcp.port={self.tcp_port}
            -E path.logs={self.logs_path}
            -E path.data={self.works_path}
            -E cluster.name={self.cluster_name}
            -E network.host='{self.network_publish_host}'
            -E index.store.type={self.index_store_type}
        """

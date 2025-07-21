"""A Jupyter server extension for managing Dask clusters."""

import os

from jupyter_server.utils import url_path_join
from loguru import logger

try:
    logger.add("/var/log/dask_labextension.log", rotation="32 MB")
except PermissionError:
    logger.add(
        os.path.join(os.path.dirname(__file__), "dask_labextension.log"),
        rotation="32 MB",
    )

from . import config
from ._version import get_versions
from .clusterhandler import DaskClusterHandler, DaskFactoriesHandler
from .dashboardhandler import DaskDashboardCheckHandler, DaskDashboardHandler

def _jupyter_labextension_paths():
    return [
        {
            "src": "labextension",
            "dest": "dask-labextension",
        }
    ]


def _jupyter_server_extension_paths():
    return [{"module": "dask_labextension"}]


def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    logger.debug("[load_jupyter_server_extension]")

    cluster_id_regex = r"(?P<cluster_id>\w+-\w+-\w+-\w+-\w+)"
    web_app = nb_server_app.web_app
    base_url = web_app.settings["base_url"]
    get_factories = url_path_join(base_url, "dask/clusters/factories")
    get_cluster_path = url_path_join(base_url, "dask/clusters/" + cluster_id_regex)
    list_clusters_path = url_path_join(base_url, "dask/clusters/" + "?")
    get_dashboard_path = url_path_join(
        base_url, f"dask/dashboard/{cluster_id_regex}(?P<proxied_path>.+)"
    )
    check_dashboard_path = url_path_join(base_url, "dask/dashboard-check/(?P<url>.+)")
    handlers = [
        (get_factories, DaskFactoriesHandler),
        (get_cluster_path, DaskClusterHandler),
        (list_clusters_path, DaskClusterHandler),
        (get_dashboard_path, DaskDashboardHandler),
        (check_dashboard_path, DaskDashboardCheckHandler),
    ]
    web_app.add_handlers(".*$", handlers)

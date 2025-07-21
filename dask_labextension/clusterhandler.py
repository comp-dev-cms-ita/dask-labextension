"""Tornado handler for dask cluster management."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import json

from jupyter_server.base.handlers import APIHandler
from loguru import logger
from tornado import web

from .manager import manager


class DaskFactoriesHandler(APIHandler):
    """
    A tornado HTTP handler for managing dask factories.
    """

    @web.authenticated
    async def get(self) -> None:
        """
        Get lists of known factories.
        """
        logger.debug("[DaskFactoriesHandler][GET]")

        factories = manager.get_factories()
        logger.debug(f"[DaskFactoriesHandler][GET][factories: {factories}]")

        self.set_status(200)
        self.finish(json.dumps({"factories": factories}))


class DaskClusterHandler(APIHandler):
    """
    A tornado HTTP handler for managing dask clusters.
    """

    @web.authenticated
    async def delete(self, cluster_id: str) -> None:
        """
        Delete a cluster by id.
        """
        logger.debug(f"[DaskClusterHandler][DELETE][cluster_id: {cluster_id}]")
        try:  # to delete the cluster.
            val = await manager.close_cluster(cluster_id)
            if val is None:
                raise web.HTTPError(404, f"Dask cluster {cluster_id} not found")
            self.set_status(204)
            self.finish()
        except Exception as e:
            raise web.HTTPError(500, str(e))

    @web.authenticated
    async def get(self, cluster_id: str = "") -> None:
        """
        Get a cluster by id. If no id is given, lists known clusters.
        """
        logger.debug(f"[DaskClusterHandler][GET][cluster_id: {cluster_id}]")
        if cluster_id == "":
            cluster_list = manager.list_clusters()
            logger.debug(f"[DaskClusterHandler][GET][cluster_list: {cluster_list}]")
            self.set_status(200)
            self.finish(json.dumps(cluster_list))
        else:
            cluster_model = manager.get_cluster(cluster_id)
            if cluster_model is None:
                raise web.HTTPError(404, f"Dask cluster {cluster_id} not found")

            self.set_status(200)
            self.finish(json.dumps(cluster_model))

    @web.authenticated
    async def put(self, cluster_id: str = "") -> None:
        """
        Create a new cluster with a given id. If no id is given, a random
        one is selected.
        """
        logger.debug(f"[DaskClusterHandler][PUT][cluster_id: {cluster_id}]")

        extra_vars = json.loads(self.request.body)

        logger.debug(f"[DaskClusterHandler][PUT][extra_vars: {extra_vars}]")

        if manager.get_cluster(cluster_id):
            raise web.HTTPError(
                403, f"A Dask cluster with ID {cluster_id} already exists!"
            )

        try:
            cluster_model = await manager.start_cluster(
                cluster_id,
                factory=extra_vars["factoryName"],
                configuration={"singularity_wn_image": extra_vars["singularityImage"],
                               "cores":  extra_vars["user_cores"],
                               "memory": extra_vars["user_memory"]
                }
            )
            self.set_status(200)
            self.finish(json.dumps(cluster_model))
        except Exception as e:
            raise web.HTTPError(500, str(e))

    @web.authenticated
    async def patch(self, cluster_id):
        """
        Scale an existing cluster."
        Not yet implemented.
        """
        logger.debug(f"[DaskClusterHandler][PATCH][cluster_id: {cluster_id}]")

        new_model = json.loads(self.request.body)
        logger.debug(f"[DaskClusterHandler][PATCH][new_model: {new_model}]")
        try:
            if new_model.get("adapt") is not None:
                cluster_model = manager.adapt_cluster(
                    cluster_id,
                    new_model["adapt"]["minimum"],
                    new_model["adapt"]["maximum"],
                )
            else:
                cluster_model = await manager.scale_cluster(
                    cluster_id, new_model["workers"]
                )
            self.set_status(200)
            self.finish(json.dumps(cluster_model))
        except Exception as e:
            raise web.HTTPError(500, str(e))
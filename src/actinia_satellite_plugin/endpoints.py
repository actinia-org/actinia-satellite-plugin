#!flask/bin/python
# -*- coding: utf-8 -*-
"""
Actinia satellite plugin endpoint definitions
"""

from flask_restful_swagger_2 import Resource

from .satellite_query import LandsatQuery, Sentinel2Query
from .ephemeral_landsat_ndvi_processor import (
    AsyncEphemeralLandsatProcessingResource,
)
from .ephemeral_sentinel2_ndvi_processor import (
    AsyncEphemeralSentinel2ProcessingResource,
    AsyncEphemeralSentinel2ProcessingResourceGCS,
)
from .persistent_landsat_timeseries_creator import (
    AsyncLandsatTimeSeriesCreatorResource,
)
from .persistent_sentinel2_timeseries_creator import (
    AsyncSentinel2TimeSeriesCreatorResource,
)
from .aws_sentinel2a_query import AWSSentinel2ADownloadLinkQuery

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"


def get_endpoint_class_name(
    endpoint_class: Resource,
    projects_url_part: str = "projects",
) -> str:
    """Create the name for the given endpoint class."""
    endpoint_class_name = endpoint_class.__name__.lower()
    if projects_url_part != "projects":
        name = f"{endpoint_class_name}_{projects_url_part}"
    else:
        name = endpoint_class_name
    return name


def create_project_endpoints(flask_api, projects_url_part="projects"):
    """
    Function to add resources with "projects" inside the endpoint url.
    Args:
        flask_api (flask_restful_swagger_2.Api): Flask api
        projects_url_part (str): The name of the projects inside the endpoint
                                 URL; to add deprecated location endpoints set
                                 it to "locations"
    """

    flask_api.add_resource(
        AsyncLandsatTimeSeriesCreatorResource,
        f"/{projects_url_part}/<string:project_name>/mapsets/"
        "<string:mapset_name>/landsat_import",
        endpoint=get_endpoint_class_name(
            AsyncLandsatTimeSeriesCreatorResource, projects_url_part
        ),
    )
    flask_api.add_resource(
        AsyncSentinel2TimeSeriesCreatorResource,
        f"/{projects_url_part}/<string:project_name>/mapsets/"
        "<string:mapset_name>/sentinel2_import",
        endpoint=get_endpoint_class_name(
            AsyncSentinel2TimeSeriesCreatorResource, projects_url_part
        ),
    )


def create_endpoints(flask_api):
    flask_api.add_resource(LandsatQuery, "/landsat_query")
    flask_api.add_resource(Sentinel2Query, "/sentinel2_query")
    flask_api.add_resource(
        AsyncEphemeralLandsatProcessingResource,
        "/landsat_process/<string:landsat_id>/"
        "<string:atcor_method>/"
        "<string:processing_method>",
    )
    flask_api.add_resource(
        AsyncEphemeralSentinel2ProcessingResourceGCS,
        "/sentinel2_process_gcs/ndvi/<string:product_id>",
    )
    flask_api.add_resource(
        AsyncEphemeralSentinel2ProcessingResource,
        "/sentinel2_process/ndvi/<string:product_id>",
    )
    flask_api.add_resource(
        AWSSentinel2ADownloadLinkQuery, "/sentinel2a_aws_query"
    )
    # add deprecated location and project endpoints
    create_project_endpoints(flask_api)
    create_project_endpoints(flask_api, projects_url_part="locations")

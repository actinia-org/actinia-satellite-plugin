#!flask/bin/python
# -*- coding: utf-8 -*-
"""
Actinia satellite plugin endpoint definitions
"""

from .satellite_query import LandsatQuery, Sentinel2Query
from .ephemeral_landsat_ndvi_processor import AsyncEphemeralLandsatProcessingResource
from .ephemeral_sentinel2_ndvi_processor import AsyncEphemeralSentinel2ProcessingResource,\
    AsyncEphemeralSentinel2ProcessingResourceGCS
from .persistent_landsat_timeseries_creator import AsyncLandsatTimeSeriesCreatorResource
from .persistent_sentinel2_timeseries_creator import AsyncSentinel2TimeSeriesCreatorResource
from .aws_sentinel2a_query import AWSSentinel2ADownloadLinkQuery

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"


def create_endpoints(flask_api):
    flask_api.add_resource(LandsatQuery, '/landsat_query')
    flask_api.add_resource(Sentinel2Query, '/sentinel2_query')
    flask_api.add_resource(AsyncEphemeralLandsatProcessingResource, '/landsat_process/<string:landsat_id>/'
                                                                    '<string:atcor_method>/'
                                                                    '<string:processing_method>')
    flask_api.add_resource(AsyncEphemeralSentinel2ProcessingResourceGCS,
                           '/sentinel2_process_gcs/ndvi/<string:product_id>')
    flask_api.add_resource(AsyncEphemeralSentinel2ProcessingResource,
                           '/sentinel2_process/ndvi/<string:product_id>')
    flask_api.add_resource(AsyncLandsatTimeSeriesCreatorResource, '/locations/<string:location_name>/mapsets/'
                                                                  '<string:mapset_name>/landsat_import')
    flask_api.add_resource(AsyncSentinel2TimeSeriesCreatorResource, '/locations/<string:location_name>/mapsets/'
                                                                    '<string:mapset_name>/sentinel2_import')
    flask_api.add_resource(AWSSentinel2ADownloadLinkQuery, '/sentinel2a_aws_query')

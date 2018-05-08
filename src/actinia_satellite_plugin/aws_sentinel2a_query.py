# -*- coding: utf-8 -*-
"""
"""

from actinia_core.resources.common.config import global_config
from flask import jsonify, make_response
from actinia_core.resources.common.app import auth
from actinia_core.resources.common.logging_interface import log_api_call
from actinia_core.resources.common.response_models import SimpleResponseModel
from actinia_core.resources.common.aws_sentinel_interface import AWSSentinel2AInterface
from actinia_core.resources.resource_base import ResourceBase
from flask_restful_swagger_2 import swagger, Schema
from copy import deepcopy


__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"


class BandInformationEnty(Schema):
    type = 'object'
    properties = {
        'file_name': {
            'type': 'string',
            'description': 'The suggested file name of this band from the requested satellite scene'
        },
        'map_name': {
            'type': 'string',
            'description': 'The suggested GRASS GIS raster map name of this band from the requested satellite scene'
        },
        'public_url': {
            'type': 'string',
            'description': 'The download URl of the band from requested satellite scene'
        }
    }
    required = ['public_url', 'map_name', 'file_name']

    example = {'file_name': 'S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155_tile_1_band_B04.jp2',
               'map_name': 'S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155_tile_1_band_B04',
               'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/36/T/VT/2017/2/2/0/B04.jp2'}


class Sentinel2ATileEntry(Schema):
    type = 'object'
    properties = {
        'B01': BandInformationEnty,
        'B02': BandInformationEnty,
        'B03': BandInformationEnty,
        'B04': BandInformationEnty,
        'B05': BandInformationEnty,
        'B06': BandInformationEnty,
        'B07': BandInformationEnty,
        'B08': BandInformationEnty,
        'B09': BandInformationEnty,
        'B10': BandInformationEnty,
        'B11': BandInformationEnty,
        'B12': BandInformationEnty,
        'info': {
            'type': 'string',
            'description': 'The url to Sentinel2A scene information'
        },
        'metadata': {
            'type': 'string',
            'description': 'The url to Sentinel2A scene XML metadata'
        },
        'preview': {
            'type': 'string',
            'description': 'The url to Sentinel2A scene preview image'
        },
        'url': {
            'type': 'string',
            'description': 'The url to Sentinel2A scene root directory that contains all informations about the scene'
        },
        'timestamp': {
            'type': 'string',
            'description': 'The sensing time of the scene',
            'format': 'dateTime'
        }
    }
    required = ['info', 'metadata', 'preview', 'url', 'timestamp']

    example = {'B04': {
        'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04.jp2',
        'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04',
        'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B04.jp2'},
               'B08': {
                   'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08.jp2',
                   'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08',
                   'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B08.jp2'},
               'info': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/tileInfo.json',
               'metadata': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/metadata.xml',
               'preview': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/preview.jpg',
               'timestamp': '2015-12-07T00:33:02.634Z',
               'url': 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/57/V/XE/2015/12/7/0/'}


class Sentinel2ASceneEntry(Schema):
    type = 'object'
    properties = {
        'product_id': {
            'type': 'string',
            'description': 'The id of the Sentinel2A scene'
        },
        'tiles': {
            'type': 'array',
            'items': Sentinel2ATileEntry,
            'description': 'A list of sentinel2A scenes'
        }
    }
    required = ["product_id", "tiles"]
    example = {'product_id': 'S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155',
               'tiles': [{'B04': {
                   'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04.jp2',
                   'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04',
                   'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B04.jp2'},
                          'B08': {
                              'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08.jp2',
                              'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08',
                              'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B08.jp2'},
                          'info': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/tileInfo.json',
                          'metadata': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/metadata.xml',
                          'preview': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/preview.jpg',
                          'timestamp': '2015-12-07T00:33:02.634Z',
                          'url': 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/57/V/XE/2015/12/7/0/'}]}


class Sentinel2ASceneList(Schema):
    type = 'object'
    properties = {
        'tiles': {
            'type': 'array',
            'items': Sentinel2ASceneEntry,
            'description': 'A list of sentinel2A scenes'
        }
    }
    example = [{'product_id': 'S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155',
                'tiles': [{'B04': {
                    'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04.jp2',
                    'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B04',
                    'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B04.jp2'},
                           'B08': {
                               'file_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08.jp2',
                               'map_name': 'S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_20151207T003302_tile_14_band_B08',
                               'public_url': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/B08.jp2'},
                           'info': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/tileInfo.json',
                           'metadata': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/metadata.xml',
                           'preview': 'http://sentinel-s2-l1c.s3.amazonaws.com/tiles/57/V/XE/2015/12/7/0/preview.jpg',
                           'timestamp': '2015-12-07T00:33:02.634Z',
                           'url': 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/#tiles/57/V/XE/2015/12/7/0/'}]}]


class Sentinel2ASceneListModel(Schema):
    """This schema defines the JSON input of the sentinel time series creator resource
    """
    type = 'object'
    properties = {
        'bands': {
            'type': 'array',
            'items': {'type': 'string'},
            'description': 'A list of band names that should be downloaded and imported for each Sentinel 2A scene.'
                           'Available are the following band names: "B01", "B02", "B03", "B04", "B05", "B06", "B07",'
                           '"B08", "B8A", "B09" "B10", "B11", "B12"'
        },
        'product_ids': {
            'type': 'array',
            'items': {'type': 'string'},
            'description': 'A list of Sentinel 2A scene names of which the tile downlad urls '
                           'and metadata infor urls should be provided.'
        }
    }
    example = {"bands": ["B04", "B08"],
               "product_ids": ["S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
                               "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_20170227T095613",
                               "S2A_MSIL1C_20170202T104241_N0204_R008_T32UNE_20170202T104236"]}

    required = ['bands', 'product_ids']


SCHEMA_DOC = {
    'tags': ['Satellite Image Algorithms'],
    'description': 'Generate the download urls for a list of sentinel2A scenes and band numbers. '
                   'Minimum required user role: user.',
    'consumes': ['application/json'],
    'parameters': [
        {
            'name': 'scenes',
            'description': 'The list of Sentinel 2A scenes and the band names',
            'required': True,
            'in': 'body',
            'schema': Sentinel2ASceneListModel
        }
    ],
    'responses': {
        '200': {
            'description': 'The result of the Sentinel 2A time series import',
            'schema': Sentinel2ASceneList
        },
        '400': {
            'description': 'The error message and a detailed log why Sentinel '
                           '2A scene downlad url creation did not succeeded',
            'schema': SimpleResponseModel
        }
    }
}


class AWSSentinel2ADownloadLinkQuery(ResourceBase):
    """Query AWS Sentinel2A archives to receive scene download links
    """
    decorators = [log_api_call, auth.login_required]

    @swagger.doc(deepcopy(SCHEMA_DOC))
    def post(self):
        """Generate the download urls for a list of sentinel2A scenes and band numbers."""

        try:
            iface = AWSSentinel2AInterface(global_config)

            rdc = self.preprocess(has_json=True, has_xml=False)

            result = iface.get_sentinel_urls(product_ids=rdc.request_data["product_ids"],
                                             bands=rdc.request_data["bands"])

            return make_response(jsonify(result), 200)
        except Exception as e:
            result = {"status": "error", "message": str(e)}
            return make_response(jsonify(result), 400)

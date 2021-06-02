# -*- coding: utf-8 -*-
"""
This module is responsible to answer requests for file based resources.
"""

from flask import jsonify, make_response
from flask_restful import Resource
from flask_restful_swagger_2 import swagger, Schema
from copy import deepcopy
from flask_restful import reqparse
from actinia_core.core.common.config import global_config
from actinia_core.core.common.google_satellite_bigquery_interface import GoogleSatelliteBigQueryInterface
from actinia_core.core.common.app import auth
from actinia_core.core.common.api_logger import log_api_call
from actinia_core.models.response_models import SimpleResponseModel

__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__      = "soerengebbert@googlemail.com"


class SatelliteSceneEntry(Schema):
    type = 'object'
    properties = {
        'scene_id': {
            'type': 'string',
            'description': 'The id of the satellite scene'
        },
        'sensing_time': {
            'type': 'string',
            'description': 'The sensing time of the scene',
        },
        'cloud_cover': {
            'type': 'number',
            'format': 'double',
            'description': 'Cloud cover of the scene 0-100'
        },
        'east_lon': {
            'type': 'number',
            'format': 'double',
            'description': 'Eastern border of the scene'
        },
        'west_lon': {
            'type': 'number',
            'format': 'double',
            'description': 'Western border of the scene'
        },
        'north_lat': {
            'type': 'number',
            'format': 'double',
            'description': 'Northern border of the scene'
        },
        'south_lat': {
            'type': 'number',
            'format': 'double',
            'description': 'Southern border of the scene'
        },
        'total_size': {
            'type': 'number',
            'format': 'double',
            'description': 'Total size of the scene'
        }
    }
    required = ['scene_id', 'sensing_time']

    example =   {
        "cloud_cover": "100.0",
        "east_lon": 117.334772,
        "north_lat": -73.8673822679,
        "scene_id": "S2A_MSIL1C_20170101T003802_N0204_R116_T50CMC_20170101T003759",
        "sensing_time": "2017-01-01T00:37:59.459000Z",
        "south_lat": -74.8755595194,
        "total_size": 608562784,
        "west_lon": 113.568673296
      }


class SatelliteSceneList(Schema):
    type = 'object'
    properties = {
        'resource_list': {
            'type': 'array',
            'items': SatelliteSceneEntry,
            'description': 'A list of satellite scenes'
        }
    }
    required = ["resource_list"]
    example = [{
        "cloud_cover": "100.0",
        "east_lon": 117.334772,
        "north_lat": -73.8673822679,
        "scene_id": "S2A_MSIL1C_20170101T003802_N0204_R116_T50CMC_20170101T003759",
        "sensing_time": "2017-01-01T00:37:59.459000Z",
        "south_lat": -74.8755595194,
        "total_size": 608562784,
        "west_lon": 113.568673296
      },
      {
        "cloud_cover": "100.0",
        "east_lon": 117.355376908,
        "north_lat": -74.7623823271,
        "scene_id": "S2A_MSIL1C_20170101T003802_N0204_R116_T50CMB_20170101T003759",
        "sensing_time": "2017-01-01T00:37:59.459000Z",
        "south_lat": -75.7719656592,
        "total_size": 604326630,
        "west_lon": 113.35802037
      }
    ]

SCHEMA_LAND_DOC={
    'tags': ['Satellite Image Algorithms'],
    'description': 'Query the Google Landsat archives using time interval, lat/lon coordinates, '
                   'scene id, spacecraft id and cloud cover. '
                   'All scenes that are located within the time interval and that intersect '
                   'the given latitude/longitude coordinates are returned as a list of '
                   'scene names with associated time stamps. '
                   'Minimum required user role: user.',
    'parameters': [
        {
            'name': 'scene_id',
            'description': 'The scene id of the landsat scenes that should be searched',
            'required': False,
            'in': 'query',
            'type': 'string'
        },
        {
            'name': 'spacecraft_id',
            'description': 'The spacecraft id of the landsat scenes that should be searched',
            'required': False,
            'in': 'query',
            'type': 'string',
            'enum': ["LANDSAT_4", "LANDSAT_5", "LANDSAT_7", "LANDSAT_8"]
        },
        {
            'name': 'start_time',
            'description': 'The start time of the search interval',
            'required': False,
            'in': 'query',
            'type': 'string',
            'format': 'dateTime'
        },
        {
            'name': 'end_time',
            'description': 'The end time of the search interval',
            'required': False,
            'in': 'query',
            'type': 'string',
            'format': 'dateTime'
        },
        {
            'name': 'lon',
            'description': 'The longitude coordinate with which the scenes should intersect',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        },
        {
            'name': 'lat',
            'description': 'The latitude coordinate with which the scenes should intersect',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        },
        {
            'name': 'cloud_covert',
            'description': 'Cloud cover between 0 - 100',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        }
    ],
    'responses': {
        '200': {
            'description': 'A list of satellite scenes that fit the search',
            'schema':SatelliteSceneList
        },
        '400': {
            'description':'The error message if the search did not succeeded',
            'schema':SimpleResponseModel
        }
    }
 }



SCHEMA_SENT_DOC={
    'tags': ['Satellite Image Algorithms'],
    'description': 'Query the Google Sentinel2 archives using time interval, lat/lon coordinates, '
                   'scene id and cloud cover. '
                   'All scenes that are located within the time interval and that intersect '
                   'the given latitude/longitude coordinates are returned as a list of '
                   'scene names with associated time stamps. '
                   'Minimum required user role: user.',
    'parameters': [
        {
            'name': 'scene_id',
            'description': 'The scene id also named product id of the Sentinel2A scenes that should be searched',
            'required': False,
            'in': 'query',
            'type': 'string'
        },
        {
            'name': 'start_time',
            'description': 'The start time of the search interval',
            'required': False,
            'in': 'query',
            'type': 'string',
            'format': 'dateTime'
        },
        {
            'name': 'end_time',
            'description': 'The end time of the search interval',
            'required': False,
            'in': 'query',
            'type': 'string',
            'format': 'dateTime'
        },
        {
            'name': 'lon',
            'description': 'The longitude coordinate with which the scenes should intersect',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        },
        {
            'name': 'lat',
            'description': 'The latitude coordinate with which the scenes should intersect',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        },
        {
            'name': 'cloud_covert',
            'description': 'Cloud cover between 0 - 100',
            'required': False,
            'in': 'query',
            'type': 'number',
            'format': 'double'
        }
    ],
    'responses': {
        '200': {
            'description': 'A list of satellite scenes that fit the search',
            'schema':SatelliteSceneList
        },
        '400': {
            'description':'The error message if the search did not succeeded',
            'schema':SimpleResponseModel
        }
    }
 }


class SatelliteQuery(Resource):
    """Query the satellites Landsat4-8 and Sentinel2A archives in the google BigQuery database
    """
    decorators = [log_api_call, auth.login_required]

    # Create a temporal module where, order, column parser
    query_parser = reqparse.RequestParser()
    query_parser.add_argument('scene_id', type=str, location='args',
                              help='Landsat scene id')
    query_parser.add_argument('spacecraft_id', type=str, location='args',
                              help='Landsat spacecraft id')
    query_parser.add_argument('start_time', type=str, location='args',
                              help='The start time of the search interval')
    query_parser.add_argument('end_time', type=str, location='args',
                              help='The end time of the search interval')
    query_parser.add_argument('lon', type=str, location='args',
                              help='The longitude coordinate with which the scenes should intersect')
    query_parser.add_argument('lat', type=str, location='args',
                              help='The latitude coordinate with which the scenes should intersect')
    query_parser.add_argument('cloud_cover', type=str, location='args',
                              help='Cloud cover between 0 - 100')

    def _get(self, satellite):

        args = self.query_parser.parse_args()

        start_time = None
        end_time = None
        lon = None
        lat = None
        cloud_cover = None
        scene_id = None
        spacecraft_id = None

        if "start_time" in args:
            start_time = args["start_time"]
        if "end_time" in args:
            end_time = args["end_time"]
        if "lon" in args:
            lon = args["lon"]
        if "lat" in args:
            lat = args["lat"]
        if "cloud_cover" in args:
            cloud_cover = args["cloud_cover"]
        if "scene_id" in args:
            scene_id = args["scene_id"]
        if "spacecraft_id" in args:
            spacecraft_id = args["spacecraft_id"]

        try:
            iface = GoogleSatelliteBigQueryInterface(global_config)

            if satellite == "landsat":
                result = iface.query_landsat_archive(start_time=start_time,
                                                     end_time=end_time,
                                                     lon=lon, lat=lat,
                                                     cloud_cover=cloud_cover,
                                                     scene_id=scene_id,
                                                     spacecraft_id=spacecraft_id)
            else:
                result = iface.query_sentinel2_archive(start_time=start_time,
                                                       end_time=end_time,
                                                       lon=lon, lat=lat,
                                                       cloud_cover=cloud_cover,
                                                       scene_id=scene_id)
            return make_response(jsonify(result), 200)
        except Exception as e:
            result = {"status": "error", "message": str(e)}
            return make_response(jsonify(result), 400)


class LandsatQuery(SatelliteQuery):
    """Query the Landsat4-8 archives
    """
    @swagger.doc(deepcopy(SCHEMA_LAND_DOC))
    def get(self):
        """Query the Google Landsat archives using time interval, lat/lon coordinates, scene id, spacecraft id and cloud cover."""
        return self._get("landsat")


class Sentinel2Query(SatelliteQuery):
    """Query the Sentinel2A archives
    """
    @swagger.doc(deepcopy(SCHEMA_SENT_DOC))
    def get(self):
        """Query the Google Sentinel2 archives using time interval, lat/lon coordinates, scene id and cloud cover."""
        return self._get("sentinel2")

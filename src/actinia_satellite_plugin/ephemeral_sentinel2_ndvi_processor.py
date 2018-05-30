# -*- coding: utf-8 -*-
"""
Asynchronous computation in specific temporary generated mapsets
with export of required map layers.
"""
import pickle
import os
import tempfile
from copy import deepcopy
from flask import jsonify, make_response
from flask_restful_swagger_2 import swagger
from actinia_core.resources.ephemeral_processing_with_export import EphemeralProcessingWithExport
from actinia_core.resources.resource_base import ResourceBase
from actinia_core.resources.common.google_satellite_bigquery_interface import GoogleSatelliteBigQueryInterface
from actinia_core.resources.common.redis_interface import enqueue_job
from actinia_core.resources.common.sentinel_processing_library import Sentinel2Processing
from actinia_core.resources.common.process_object import Process
from actinia_core.resources.common.exceptions import AsyncProcessError
from actinia_core.resources.common.response_models import UnivarResultModel, ProcessingResponseModel
from actinia_core.resources.common.app import auth
from actinia_core.resources.common.logging_interface import log_api_call
from actinia_core.resources.common.response_models import ProcessingErrorResponseModel

__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__      = "soerengebbert@googlemail.com"


class SentinelNDVIResponseModel(ProcessingResponseModel):
    """The response of the Sentinel2A vegetation index computation

    It is used as schema to define the *process_result* in a ProcessingResponseModel derivative.
    """
    type = 'object'
    properties =  deepcopy(ProcessingResponseModel.properties)
    properties["process_results"] = {}
    properties["process_results"]["type"] = "array"
    properties["process_results"]["items"] = UnivarResultModel
    required =  deepcopy(ProcessingResponseModel.required)
    example = {
      "accept_datetime": "2018-05-30 12:25:43.987713",
      "accept_timestamp": 1527683143.9877105,
      "api_info": {
        "endpoint": "asyncephemeralsentinel2processingresource",
        "method": "POST",
        "path": "/api/v1/sentinel2_process/ndvi/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749",
        "request_url": "http://localhost:8080/api/v1/sentinel2_process/ndvi/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749"
      },
      "datetime": "2018-05-30 12:29:11.800608",
      "http_code": 200,
      "message": "Processing successfully finished",
      "process_chain_list": [
        {
          "1": {
            "flags": "g",
            "inputs": {
              "map": "ndvi"
            },
            "module": "r.univar",
            "outputs": {
              "output": {
                "name": "/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/tmp7il3n0jk.univar"
              }
            }
          }
        },
        {
          "1": {
            "inputs": {
              "map": "ndvi"
            },
            "module": "d.rast"
          },
          "2": {
            "flags": "n",
            "inputs": {
              "at": "8,92,0,7",
              "raster": "ndvi"
            },
            "module": "d.legend"
          },
          "3": {
            "inputs": {
              "at": "20,4",
              "style": "line"
            },
            "module": "d.barscale"
          }
        }
      ],
      "process_log": [
        {
          "executable": "/usr/bin/wget",
          "parameter": [
            "-t5",
            "-c",
            "-q",
            "https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/50/R/KR/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749.SAFE/GRANULE/L1C_T50RKR_A007608_20161206T030749/IMG_DATA/T50RKR_20161206T030112_B08.jp2"
          ],
          "return_code": 0,
          "run_time": 49.85953092575073,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/usr/bin/wget",
          "parameter": [
            "-t5",
            "-c",
            "-q",
            "https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles/50/R/KR/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749.SAFE/GRANULE/L1C_T50RKR_A007608_20161206T030749/IMG_DATA/T50RKR_20161206T030112_B04.jp2"
          ],
          "return_code": 0,
          "run_time": 38.676433801651,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749.gml",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749.gml"
          ],
          "return_code": 0,
          "run_time": 0.05118393898010254,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/T50RKR_20161206T030112_B08.jp2",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08"
          ],
          "return_code": 0,
          "run_time": 0.35857558250427246,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/T50RKR_20161206T030112_B04.jp2",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04"
          ],
          "return_code": 0,
          "run_time": 0.15271401405334473,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "python2",
          "parameter": [
            "/usr/local/bin/grass75",
            "-e",
            "-c",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08",
            "/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/sentinel2"
          ],
          "return_code": 0,
          "run_time": 0.36118006706237793,
          "stderr": [
            "Default locale settings are missing. GRASS running with C locale.WARNING: Searched for a web browser, but none found",
            "Creating new GRASS GIS location/mapset...",
            "Cleaning up temporary files...",
            ""
          ],
          "stdout": "Default locale not found, using UTF-8\n"
        },
        {
          "executable": "v.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749.gml",
            "output=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749",
            "--q"
          ],
          "return_code": 0,
          "run_time": 0.3551313877105713,
          "stderr": [
            "WARNING: Projection of dataset does not appear to match current location.",
            "",
            "Location PROJ_INFO is:",
            "name: WGS 84 / UTM zone 50N",
            "datum: wgs84",
            "ellps: wgs84",
            "proj: utm",
            "zone: 50",
            "no_defs: defined",
            "",
            "Dataset PROJ_INFO is:",
            "name: WGS 84",
            "datum: wgs84",
            "ellps: wgs84",
            "proj: ll",
            "no_defs: defined",
            "",
            "ERROR: proj",
            "",
            "WARNING: Width for column fid set to 255 (was not specified by OGR), some strings may be truncated!",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "v.timestamp",
          "parameter": [
            "map=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749",
            "date=06 dec 2016 03:07:49"
          ],
          "return_code": 0,
          "run_time": 0.050455570220947266,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/usr/bin/gdal_translate",
          "parameter": [
            "-projwin",
            "113.949663",
            "28.011816",
            "115.082607",
            "27.001706",
            "-of",
            "vrt",
            "-projwin_srs",
            "EPSG:4326",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08.vrt"
          ],
          "return_code": 0,
          "run_time": 0.05114293098449707,
          "stderr": [
            "Warning 1: Computed -srcwin 5 -225 10971 11419 falls partially outside raster extent. Going on however.",
            ""
          ],
          "stdout": "Input file size is 10980, 10980\n"
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08.vrt",
            "output=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08_uncropped",
            "--q"
          ],
          "return_code": 0,
          "run_time": 16.326167583465576,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "align=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08_uncropped",
            "vector=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749",
            "-g"
          ],
          "return_code": 0,
          "run_time": 0.10460591316223145,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=50\nn=3100030\ns=2990100\nw=199960\ne=309790\nnsres=10\newres=10\nrows=10993\ncols=10983\ncells=120736119\n"
        },
        {
          "executable": "r.mask",
          "parameter": [
            "vector=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749"
          ],
          "return_code": 0,
          "run_time": 7.36047887802124,
          "stderr": [
            "Reading areas...",
            "0..100",
            "Writing raster map...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            "Reading areas...",
            "0..100",
            "Writing raster map...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            "All subsequent raster operations will be limited to the MASK area. Removing or renaming raster map named 'MASK' will restore raster operations to normal.",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mapcalc",
          "parameter": [
            "expression=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08 = float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08_uncropped)"
          ],
          "return_code": 0,
          "run_time": 10.695591926574707,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.timestamp",
          "parameter": [
            "map=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08",
            "date=06 dec 2016 03:07:49"
          ],
          "return_code": 0,
          "run_time": 0.053069353103637695,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.remove",
          "parameter": [
            "type=raster",
            "name=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08_uncropped",
            "-f"
          ],
          "return_code": 0,
          "run_time": 0.050362348556518555,
          "stderr": [
            "Removing raster <S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08_uncropped>",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mask",
          "parameter": [
            "-r"
          ],
          "return_code": 0,
          "run_time": 0.10059237480163574,
          "stderr": [
            "Raster MASK removed",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/usr/bin/gdal_translate",
          "parameter": [
            "-projwin",
            "113.949663",
            "28.011816",
            "115.082607",
            "27.001706",
            "-of",
            "vrt",
            "-projwin_srs",
            "EPSG:4326",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04",
            "/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04.vrt"
          ],
          "return_code": 0,
          "run_time": 0.05096769332885742,
          "stderr": [
            "Warning 1: Computed -srcwin 5 -225 10971 11419 falls partially outside raster extent. Going on however.",
            ""
          ],
          "stdout": "Input file size is 10980, 10980\n"
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04.vrt",
            "output=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04_uncropped",
            "--q"
          ],
          "return_code": 0,
          "run_time": 16.76022958755493,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "align=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04_uncropped",
            "vector=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749",
            "-g"
          ],
          "return_code": 0,
          "run_time": 0.0505826473236084,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=50\nn=3100030\ns=2990100\nw=199960\ne=309790\nnsres=10\newres=10\nrows=10993\ncols=10983\ncells=120736119\n"
        },
        {
          "executable": "r.mask",
          "parameter": [
            "vector=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749"
          ],
          "return_code": 0,
          "run_time": 6.779608249664307,
          "stderr": [
            "Reading areas...",
            "0..100",
            "Writing raster map...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            "Reading areas...",
            "0..100",
            "Writing raster map...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            "All subsequent raster operations will be limited to the MASK area. Removing or renaming raster map named 'MASK' will restore raster operations to normal.",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mapcalc",
          "parameter": [
            "expression=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04 = float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04_uncropped)"
          ],
          "return_code": 0,
          "run_time": 10.141529321670532,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.timestamp",
          "parameter": [
            "map=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04",
            "date=06 dec 2016 03:07:49"
          ],
          "return_code": 0,
          "run_time": 0.05050253868103027,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.remove",
          "parameter": [
            "type=raster",
            "name=S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04_uncropped",
            "-f"
          ],
          "return_code": 0,
          "run_time": 0.05098080635070801,
          "stderr": [
            "Removing raster <S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04_uncropped>",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mask",
          "parameter": [
            "-r"
          ],
          "return_code": 0,
          "run_time": 0.10424232482910156,
          "stderr": [
            "Raster MASK removed",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mapcalc",
          "parameter": [
            "expression=ndvi = (float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08) - float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04))/(float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B08) + float(S2A_MSIL1C_20161206T030112_N0204_R032_T50RKR_20161206T030749_B04))"
          ],
          "return_code": 0,
          "run_time": 20.28681755065918,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.colors",
          "parameter": [
            "color=ndvi",
            "map=ndvi"
          ],
          "return_code": 0,
          "run_time": 0.05031251907348633,
          "stderr": [
            "Color table for raster map <ndvi> set to 'ndvi'",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.univar",
          "parameter": [
            "map=ndvi",
            "output=/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/tmp7il3n0jk.univar",
            "-g"
          ],
          "return_code": 0,
          "run_time": 4.54892897605896,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "d.rast",
          "parameter": [
            "map=ndvi"
          ],
          "return_code": 0,
          "run_time": 2.0198700428009033,
          "stderr": [
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "d.legend",
          "parameter": [
            "raster=ndvi",
            "at=8,92,0,7",
            "-n"
          ],
          "return_code": 0,
          "run_time": 0.4614551067352295,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "d.barscale",
          "parameter": [
            "style=line",
            "at=20,4"
          ],
          "return_code": 0,
          "run_time": 0.416748046875,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "raster=ndvi",
            "-g"
          ],
          "return_code": 0,
          "run_time": 0.051720619201660156,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=50\nn=3100030\ns=2990100\nw=199960\ne=309790\nnsres=10\newres=10\nrows=10993\ncols=10983\ncells=120736119\n"
        },
        {
          "executable": "r.out.gdal",
          "parameter": [
            "-fm",
            "input=ndvi",
            "format=GTiff",
            "createopt=COMPRESS=LZW",
            "output=/actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/ndvi.tiff"
          ],
          "return_code": 0,
          "run_time": 12.550397157669067,
          "stderr": [
            "Checking GDAL data type and nodata value...",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "Using GDAL data type <Float32>",
            "Input raster map contains cells with NULL-value (no-data). The value -nan will be used to represent no-data values in the input map. You can specify a nodata value with the nodata option.",
            "Exporting raster data to GTiff format...",
            "ERROR 6: SetColorTable() only supported for Byte or UInt16 bands in TIFF format.",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "r.out.gdal complete. File </actinia/workspace/temp_db/gisdbase_103a050c380e4f50b36efd3f77bd1419/.tmp/ndvi.tiff> created.",
            ""
          ],
          "stdout": ""
        }
      ],
      "process_results": [
        {
          "cells": 120736119.0,
          "coeff_var": 39.2111992829072,
          "max": 0.80298912525177,
          "mean": 0.345280366103636,
          "mean_of_abs": 0.347984182813063,
          "min": -0.96863466501236,
          "n": 120371030.0,
          "name": "ndvi",
          "null_cells": 365089.0,
          "range": 1.77162379026413,
          "stddev": 0.135388572437648,
          "sum": 41561753.3066718,
          "variance": 0.0183300655467043
        }
      ],
      "progress": {
        "num_of_steps": 33,
        "step": 32
      },
      "resource_id": "resource_id-6b849585-576f-40b5-a514-34a7cf1f97ce",
      "status": "finished",
      "time_delta": 207.813636302948,
      "timestamp": 1527683351.8002071,
      "urls": {
        "resources": [
          "http://localhost:8080/api/v1/resource/superadmin/resource_id-6b849585-576f-40b5-a514-34a7cf1f97ce/tmpsaeegg0q.png",
          "http://localhost:8080/api/v1/resource/superadmin/resource_id-6b849585-576f-40b5-a514-34a7cf1f97ce/ndvi.tiff"
        ],
        "status": "http://localhost:8080/api/v1/resources/superadmin/resource_id-6b849585-576f-40b5-a514-34a7cf1f97ce"
      },
      "user_id": "superadmin"
    }
    # required.append("process_results")


SWAGGER_DOC = {
    'tags': ['Satellite Image Algorithms'],
    'description': 'NDVI computation of an arbitrary Sentinel 2A scene.'
                   'The processing is as follows: A user specific Sentinel 2A scene (Bands 04 and 08)'
                   'will be download and imported into an ephemeral database.. '
                   'The NDVI will be computed via r.mapcalc. '
                   'The result of the computation is available as gzipped geotiff file. In addition, '
                   'the univariate statistic will be computed '
                   'as well as a preview image including a legend and scale.'
                   ' Minimum required user role: user.',
    'parameters': [
        {
            'name': 'product_id',
            'description': 'The product id of a sentinel scene',
            'required': True,
            'in': 'path',
            'type': 'string',
            'default': 'S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138'
        }
    ],
    'responses': {
        '200': {
            'description': 'This response includes all created resources '
                           'as URL as well as the processing log and other metadata.',
            'schema':SentinelNDVIResponseModel
        },
        '400': {
            'description': 'The error message and a detailed log why NDVI processing of '
                           'a sentinel2 scene did not succeeded',
            'schema':ProcessingErrorResponseModel
        }
    }
}


def extract_sensor_id_from_scene_id(scene_id):
    """Extract the sensor id from a sentinel2 scene id

    Args:
        scene_id (str): The sentinel2 scene id

    Returns:
        (str)
        The sencor id

    """
    return "%s0%s"%(scene_id[0:2], scene_id[2:3])


class AsyncEphemeralSentinel2ProcessingResource(ResourceBase):
    """
    This class represents a resource that runs asynchronous processing tasks
    to download and process Sentinel 2A satellite images in an ephemeral GRASS location
    and stores the result in a network storage like GlusterFS or NFS
    """
    decorators = [log_api_call, auth.login_required]

    def __init__(self):
        ResourceBase.__init__(self)
        self.response_model_class = SentinelNDVIResponseModel

    @swagger.doc(deepcopy(SWAGGER_DOC))
    def post(self, product_id):
        """NDVI computation of an arbitrary Sentinel 2A scene.
        """

        rdc = self.preprocess(has_json=False, location_name="sentinel2")
        rdc.set_user_data(product_id)

        enqueue_job(self.job_timeout, start_job, rdc)
        html_code, response_model = pickle.loads(self.response_data)
        return make_response(jsonify(response_model), html_code)


class AsyncEphemeralSentinel2ProcessingResourceGCS(ResourceBase):
    """
    This class represents a resource that runs asynchronous processing tasks
    to download and process Sentinel 2A satellite images in an ephemeral GRASS location
    and uploads the result to a google cloud storage bucket.
    """
    decorators = [log_api_call, auth.login_required]

    def __init__(self):
        ResourceBase.__init__(self)
        self.response_model_class = SentinelNDVIResponseModel

    @swagger.doc(deepcopy(SWAGGER_DOC))
    def post(self, product_id):
        """NDVI computation of an arbitrary Sentinel 2A scene string the result in Google Cloud Storage
        """
        rdc = self.preprocess(has_json=False, location_name="sentinel2")
        rdc.set_user_data(product_id)
        rdc.set_storage_model_to_gcs()

        enqueue_job(self.job_timeout, start_job, rdc)
        html_code, response_model = pickle.loads(self.response_data)
        return make_response(jsonify(response_model), html_code)


def start_job(*args):
    processing = EphemeralSentinelProcessing(*args)
    processing.run()


class EphemeralSentinelProcessing(EphemeralProcessingWithExport):
    """
    """
    def __init__(self,rdc):
        """
        Setup the variables of this class

        Args:
            rdc (ResourceDataContainer): The data container that contains all required variables for processing

        """
        EphemeralProcessingWithExport.__init__(self, rdc)

        self.query_interface = GoogleSatelliteBigQueryInterface(self.config)

        self.product_id = self.rdc.user_data
        self.sentinel2_band_file_list = {}
        self.gml_footprint = ""
        self.user_download_cache_path = os.path.join(self.config.DOWNLOAD_CACHE, self.user_id)
        self.raster_result_list = []               # The raster layer names which must be exported, stats computed
                                                   # and preview image created
        self.module_results = []                   # A list of r.univar output classes for each vegetation index
        self.response_model_class = SentinelNDVIResponseModel   # The class that is used to create the response
        self.required_bands = ["B08", "B04"]       # The Sentinel 2A bands that are required for NDVI processing
        self.query_result = None

    def _prepare_sentinel2_download(self):
        """Check the download cache if the file already exists, to avoid redundant downloads.
        The downloaded files will be stored in a temporary directory. After the download of all files
        completes, the downloaded files will be moved to the download cache. This avoids broken
        files in case a download was interrupted or stopped by termination.

        """
        # Create the download cache directory if it does not exists
        if os.path.exists(self.config.DOWNLOAD_CACHE):
            pass
        else:
            os.mkdir(self.config.DOWNLOAD_CACHE)

        # Create the user specific download cache directory to put the downloaded files into it
        if os.path.exists(self.user_download_cache_path):
            pass
        else:
            os.mkdir(self.user_download_cache_path)

        # Switch into the tempfile directory
        os.chdir(self.temp_file_path)

        # We have to set the home directory to create the grass location
        os.putenv("HOME", "/tmp")

        try:
            self.query_result = self.query_interface.get_sentinel_urls([self.product_id,], self.required_bands)
        except Exception as e:
            raise AsyncProcessError("Error in querying Sentinel 2A product <%s> "
                                    "in Google BigQuery Sentinel 2A database. "
                                    "Error: %s"%(self.product_id, str(e)))

        if not self.query_result:
            raise AsyncProcessError("Unable to find Sentinel 2A product <%s> "
                                    "in Google BigQuery Sentinel 2A database"%self.product_id)

    def _create_temp_database(self, mapsets=[]):
        """Create a temporary gis database and location with a PERMANENT mapset for processing

        Raises:
            This function raises AsyncProcessError in case of an error.

        """
        if not self.sentinel2_band_file_list:
            raise AsyncProcessError("Unable to create a temporary GIS database, no data is available")

        try:
            geofile = self.sentinel2_band_file_list[self.required_bands[0]][0]
            self._send_resource_update(geofile)
            # We have to set the home directory to create the grass location
            os.putenv("HOME", "/tmp")

            # Switch into the GRASS temporary database directory
            os.chdir(self.temp_grass_data_base)

            executable_params = list()
            executable_params.append(self.config.GRASS_GIS_START_SCRIPT)
            executable_params.append("-e")
            executable_params.append("-c")
            executable_params.append(geofile)
            executable_params.append(os.path.join(self.temp_grass_data_base, self.location_name))

            self.message_logger.info("%s %s"%(self.config.GRASS_GIS_START_SCRIPT, executable_params))

            self._update_num_of_steps(1)

            p = Process(exec_type="exec",
                        executable="python2",
                        executable_params=executable_params)

            # Create the GRASS location, this will create the location and mapset paths
            self._run_process(p)
        except Exception as e:
            raise AsyncProcessError("Unable to create a temporary GIS database and location at <%s>"
                                    ", Exception: %s"%(os.path.join(self.temp_grass_data_base,
                                                                    self.location_name, "PERMANENT"),
                                                       str(e)))

    def _run_r_univar_command(self, raster_name):
        """Compute the univariate statistics for a raster layer
        and put the result as dict in the module_result dict

        Args:
            raster_name:

        """
        result_file = tempfile.mktemp(suffix=".univar", dir=self.temp_file_path)
        univar_command = dict()
        univar_command["1"] = {"module":"r.univar",
                               "inputs":{"map":raster_name},
                               "outputs":{"output":{"name":result_file}},
                               "flags":"g"}

        process_list = self._validate_process_chain(process_chain=univar_command,
                                                    skip_permission_check=True)
        self._execute_process_list(process_list=process_list)

        result_list = open(result_file, "r").readlines()
        results = {"name":raster_name}

        for line in result_list:
            if "=" in line:
                key, value = line.split("=")
                results[key] = float(value.strip())

        self.module_results.append(UnivarResultModel(**results))

    def _render_preview_image(self, raster_name):
        """Setup the render environment and create a g.region
         process chain entry to setup the extent from the options.

        Args:
            options: The parser options that contain n, s, e and w entries for region settings
            result_file: The resulting PNG file name

        Returns:
            A process chain entry of g.region

        """
        result_file = tempfile.mktemp(suffix=".png", dir=self.temp_file_path)

        os.putenv("GRASS_RENDER_IMMEDIATE", "png")
        os.putenv("GRASS_RENDER_WIDTH", "1300")
        os.putenv("GRASS_RENDER_HEIGHT", "1000")
        os.putenv("GRASS_RENDER_TRANSPARENT", "TRUE")
        os.putenv("GRASS_RENDER_TRUECOLOR", "TRUE")
        os.putenv("GRASS_RENDER_FILE", result_file)
        os.putenv("GRASS_RENDER_FILE_READ", "TRUE")

        render_commands = {}
        render_commands["1"] = {"module":"d.rast","inputs":{"map":raster_name}}
                               # "flags":"n"}

        render_commands["2"] = {"module":"d.legend","inputs":{"raster":raster_name,
                                                              "at":"8,92,0,7"},
                                "flags":"n"}

        render_commands["3"] = {"module":"d.barscale","inputs":{"style":"line",
                                                                "at":"20,4"},
                                # "flags":"n"   # No "n" flag in grass72
                                }

        # Run the selected modules
        process_list = self._validate_process_chain(process_chain=render_commands,
                                                    skip_permission_check=True)
        self._execute_process_list(process_list)

        # Attach the png preview image to the resource URL list
        # Generate the resource URL's from the url base and the file name
        # Copy the png file to the resource directory
        # file_name = "%s_preview.png"%(raster_name)
        # resource_url = self.resource_url_base.replace("__None__", file_name)
        # self.storage_interface.
        # self.resource_url_list.append(resource_url)
        # export_path = os.path.join(self.resource_export_path, file_name)
        # shutil.move(result_file, export_path)

        # Store the temporary file in the resource storage
        # and receive the resource URL
        resource_url = self.storage_interface.store_resource(result_file)
        self.resource_url_list.append(resource_url)

    def _create_output_resources(self, raster_result_list):
        """Create the output resources from the raster layer that are the result of the processing

        The following resources will be computed

        - Univariate statistics as result dictionary for each raster layer
        - A PNG preview image for each raster layer
        - A gzipped GeoTiff file

        """

        for raster_name in raster_result_list:
            self._run_r_univar_command(raster_name)
            # Render a preview image for this raster layer
            self._render_preview_image(raster_name)
            export_dict = {"name":raster_name,
                           "export":{"format":"GTiff",
                                     "type":"raster"}}
            # Add the raster layer to the export list
            self.resource_export_list.append(export_dict)

        self._update_num_of_steps(len(raster_result_list))

        # Export all resources and generate the finish response
        self._export_resources(use_raster_region=True)

    def _execute(self):
        """Overwrite this function in subclasses

            - Setup user credentials and working paths
            - Create the resource directory
            - Download and store the sentinel2 scene files
            - Initialize and create the temporal database and location
            - Analyse the process chains
            - Run the modules
            - Export the results
            - Cleanup

        """
        # Setup the user credentials and logger
        self._setup()

        # Create and check the resource directory
        self.storage_interface.setup()

        # Setup the download cache
        self._prepare_sentinel2_download()

        process_lib = Sentinel2Processing(self.config,
                                          self.product_id,
                                          self.query_result,
                                          self.required_bands,
                                          self.temp_file_path,
                                          self.user_download_cache_path,
                                          self._send_resource_update,
                                          self.message_logger)

        download_commands, self.sentinel2_band_file_list = process_lib.get_sentinel2_download_process_list()

        # Download the sentinel scene if not in the download cache
        if download_commands:
            self._update_num_of_steps(len(download_commands))
            self._execute_process_list(process_list=download_commands)

        # Setup GRASS
        self._create_temporary_grass_environment(source_mapset_name="PERMANENT")

        # Import and prepare the sentinel scenes
        import_commands = process_lib.get_sentinel2_import_process_list()
        self._update_num_of_steps(len(import_commands))
        self._execute_process_list(process_list=import_commands)

        # Generate the ndvi command
        nir = self.sentinel2_band_file_list["B08"][1]
        red = self.sentinel2_band_file_list["B04"][1]
        ndvi_commands = process_lib.get_ndvi_r_mapcalc_process_list(red, nir, "ndvi")
        self._update_num_of_steps(len(ndvi_commands))
        self._execute_process_list(process_list=ndvi_commands)

        self.raster_result_list.append("ndvi")

        # Create the output resources: stats, preview and geotiff
        self._create_output_resources(self.raster_result_list)

    def _final_cleanup(self):
        """Overwrite this function in subclasses to perform the final cleanup
        """
        # Clean up and remove the temporary gisdbase
        self._cleanup()
        # Remove resource directories
        if "error" in self.run_state or "terminated" in self.run_state:
            self.storage_interface.remove_resources()

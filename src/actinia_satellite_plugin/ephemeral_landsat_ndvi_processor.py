# -*- coding: utf-8 -*-
import pickle
import os
import tempfile
from copy import deepcopy
from flask import jsonify, make_response
from actinia_core.core.common.process_object import Process
from actinia_core.core.common.app import auth
from actinia_core.core.common.api_logger import log_api_call
from flask_restful_swagger_2 import swagger
from actinia_core.rest.resource_base import ResourceBase
from actinia_core.rest.ephemeral_processing_with_export import EphemeralProcessingWithExport
from actinia_core.core.common.redis_interface import enqueue_job
from actinia_core.core.common.exceptions import AsyncProcessError
from actinia_core.models.response_models import UnivarResultModel, ProcessingResponseModel
from actinia_core.core.common.landsat_processing_library import LandsatProcessing
from actinia_core.models.response_models import ProcessingErrorResponseModel
from actinia_api import URL_PREFIX

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"

SCENE_SUFFIXES = {
    "LT04": ["_B1.TIF", "_B2.TIF", "_B3.TIF", "_B4.TIF", "_B5.TIF", "_B6.TIF", "_B7.TIF", "_MTL.txt"],
    "LT05": ["_B1.TIF", "_B2.TIF", "_B3.TIF", "_B4.TIF", "_B5.TIF", "_B6.TIF", "_B7.TIF", "_MTL.txt"],
    "LE07": ["_B1.TIF", "_B2.TIF", "_B3.TIF", "_B4.TIF", "_B5.TIF", "_B6_VCID_2.TIF", "_B6_VCID_1.TIF",
             "_B7.TIF", "_B8.TIF", "_MTL.txt"],
    "LC08": ["_B1.TIF", "_B2.TIF", "_B3.TIF", "_B4.TIF", "_B5.TIF", "_B6.TIF", "_B7.TIF",
             "_B8.TIF", "_B9.TIF", "_B10.TIF", "_B11.TIF", "_MTL.txt"]}

RASTER_SUFFIXES = {
    "LT04": [".1", ".2", ".3", ".4", ".5", ".6", ".7"],
    "LT05": [".1", ".2", ".3", ".4", ".5", ".6", ".7"],
    "LE07": [".1", ".2", ".3", ".4", ".5", ".61", ".62", ".7", ".8"],
    "LC08": [".1", ".2", ".3", ".4", ".5", ".6", ".7", ".8", ".9", ".10", ".11"]}


class LandsatNDVIResponseModel(ProcessingResponseModel):
    """The response of the Landsat vegetation index computation

    It is used as schema to define the *process_result* in a ProcessingResponseModel derivative.
    """
    type = 'object'
    properties = deepcopy(ProcessingResponseModel.properties)
    properties["process_results"] = {}
    properties["process_results"]["type"] = "array"
    properties["process_results"]["items"] = UnivarResultModel
    required = deepcopy(ProcessingResponseModel.required)
    example = {
      "accept_datetime": "2018-05-30 11:16:03.033305",
      "accept_timestamp": 1527678963.033304,
      "api_info": {
        "endpoint": "asyncephemerallandsatprocessingresource",
        "method": "POST",
        "path": f"{URL_PREFIX}/landsat_process/LC80440342016259LGN00/TOAR/NDVI",
        "request_url": f"http://localhost:5000{URL_PREFIX}/landsat_process/LC80440342016259LGN00/TOAR/NDVI"
      },
      "datetime": "2018-05-30 11:22:58.315162",
      "http_code": 200,
      "message": "Processing successfully finished",
      "process_chain_list": [
        {
          "1": {
            "flags": "g",
            "inputs": {
              "map": "LC80440342016259LGN00_TOAR_NDVI"
            },
            "module": "r.univar",
            "outputs": {
              "output": {
                "name": "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/tmpkiv0uv6z.univar"
              }
            }
          }
        },
        {
          "1": {
            "flags": "n",
            "inputs": {
              "map": "LC80440342016259LGN00_TOAR_NDVI"
            },
            "module": "d.rast"
          },
          "2": {
            "flags": "n",
            "inputs": {
              "at": "8,92,0,7",
              "raster": "LC80440342016259LGN00_TOAR_NDVI"
            },
            "module": "d.legend"
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B6.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B6.TIF"
          ],
          "return_code": 0,
          "run_time": 23.63347291946411,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B6.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B6.TIF"
          ],
          "return_code": 0,
          "run_time": 0.05022144317626953,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B7.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B7.TIF"
          ],
          "return_code": 0,
          "run_time": 22.89448094367981,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B7.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B7.TIF"
          ],
          "return_code": 0,
          "run_time": 0.051961421966552734,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B8.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B8.TIF"
          ],
          "return_code": 0,
          "run_time": 83.04966020584106,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B8.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B8.TIF"
          ],
          "return_code": 0,
          "run_time": 0.05012321472167969,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B9.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B9.TIF"
          ],
          "return_code": 0,
          "run_time": 11.948487043380737,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B9.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B9.TIF"
          ],
          "return_code": 0,
          "run_time": 0.05081939697265625,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B10.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B10.TIF"
          ],
          "return_code": 0,
          "run_time": 15.688527345657349,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B10.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B10.TIF"
          ],
          "return_code": 0,
          "run_time": 0.05163097381591797,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B11.TIF",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B11.TIF"
          ],
          "return_code": 0,
          "run_time": 15.100370645523071,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_B11.TIF",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B11.TIF"
          ],
          "return_code": 0,
          "run_time": 0.05057358741760254,
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
            "-O",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_MTL.txt",
            "https://storage.googleapis.com/gcp-public-data-landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_MTL.txt"
          ],
          "return_code": 0,
          "run_time": 0.25395917892456055,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/mv",
          "parameter": [
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_MTL.txt",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_MTL.txt"
          ],
          "return_code": 0,
          "run_time": 0.05015206336975098,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "python3",
          "parameter": [
            "/usr/local/bin/grass79",
            "-e",
            "-c",
            "/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B1.TIF",
            "/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/Landsat"
          ],
          "return_code": 0,
          "run_time": 0.15161657333374023,
          "stderr": [
            "Default locale settings are missing. GRASS running with C locale.WARNING: Searched for a web browser, but none found",
            "Creating new GRASS GIS location/mapset...",
            "Cleaning up temporary files...",
            ""
          ],
          "stdout": "Default locale not found, using UTF-8\n"
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B1.TIF",
            "output=LC80440342016259LGN00.1",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.093010902404785,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B2.TIF",
            "output=LC80440342016259LGN00.2",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.020535707473755,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B3.TIF",
            "output=LC80440342016259LGN00.3",
            "--q"
          ],
          "return_code": 0,
          "run_time": 2.9988090991973877,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B4.TIF",
            "output=LC80440342016259LGN00.4",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.0504379272460938,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B5.TIF",
            "output=LC80440342016259LGN00.5",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.0378293991088867,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B6.TIF",
            "output=LC80440342016259LGN00.6",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.1231300830841064,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B7.TIF",
            "output=LC80440342016259LGN00.7",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.0385892391204834,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B8.TIF",
            "output=LC80440342016259LGN00.8",
            "--q"
          ],
          "return_code": 0,
          "run_time": 11.727607488632202,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B9.TIF",
            "output=LC80440342016259LGN00.9",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.531238317489624,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B10.TIF",
            "output=LC80440342016259LGN00.10",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.1895594596862793,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_B11.TIF",
            "output=LC80440342016259LGN00.11",
            "--q"
          ],
          "return_code": 0,
          "run_time": 3.1583566665649414,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "i.landsat.toar",
          "parameter": [
            "input=LC80440342016259LGN00.",
            "metfile=/actinia/workspace/download_cache/superadmin/LC80440342016259LGN00_MTL.txt",
            "method=uncorrected",
            "output=LC80440342016259LGN00_TOAR.",
            "--q"
          ],
          "return_code": 0,
          "run_time": 101.34896063804626,
          "stderr": [
            "WARNING: ESUN evaluated from REFLECTANCE_MAXIMUM_BAND",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "i.vi",
          "parameter": [
            "red=LC80440342016259LGN00_TOAR.4",
            "nir=LC80440342016259LGN00_TOAR.5",
            "green=LC80440342016259LGN00_TOAR.3",
            "blue=LC80440342016259LGN00_TOAR.2",
            "band5=LC80440342016259LGN00_TOAR.7",
            "band7=LC80440342016259LGN00_TOAR.8",
            "viname=ndvi",
            "output=LC80440342016259LGN00_TOAR_NDVI"
          ],
          "return_code": 0,
          "run_time": 45.43833112716675,
          "stderr": [
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.colors",
          "parameter": [
            "map=LC80440342016259LGN00_TOAR_NDVI",
            "color=ndvi"
          ],
          "return_code": 0,
          "run_time": 0.050219058990478516,
          "stderr": [
            "Color table for raster map <LC80440342016259LGN00_TOAR_NDVI> set to 'ndvi'",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.univar",
          "parameter": [
            "map=LC80440342016259LGN00_TOAR_NDVI",
            "output=/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/tmpkiv0uv6z.univar",
            "-g"
          ],
          "return_code": 0,
          "run_time": 2.5560226440429688,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "d.rast",
          "parameter": [
            "map=LC80440342016259LGN00_TOAR_NDVI",
            "-n"
          ],
          "return_code": 0,
          "run_time": 1.2287390232086182,
          "stderr": [
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "d.legend",
          "parameter": [
            "raster=LC80440342016259LGN00_TOAR_NDVI",
            "at=8,92,0,7",
            "-n"
          ],
          "return_code": 0,
          "run_time": 0.37291598320007324,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "raster=LC80440342016259LGN00_TOAR_NDVI",
            "-g"
          ],
          "return_code": 0,
          "run_time": 0.051508188247680664,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=10\nn=4264515\ns=4030185\nw=464385\ne=694515\nnsres=30\newres=30\nrows=7811\ncols=7671\ncells=59918181\n"
        },
        {
          "executable": "r.out.gdal",
          "parameter": [
            "-fm",
            "input=LC80440342016259LGN00_TOAR_NDVI",
            "format=GTiff",
            "createopt=COMPRESS=LZW",
            "output=/actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_TOAR_NDVI.tiff"
          ],
          "return_code": 0,
          "run_time": 8.784564018249512,
          "stderr": [
            "Checking GDAL data type and nodata value...",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "Using GDAL data type <Float64>",
            "Input raster map contains cells with NULL-value (no-data). The value -nan will be used to represent no-data values in the input map. You can specify a nodata value with the nodata option.",
            "Exporting raster data to GTiff format...",
            "ERROR 6: SetColorTable() only supported for Byte or UInt16 bands in TIFF format.",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "r.out.gdal complete. File </actinia/workspace/temp_db/gisdbase_4e879f3951334a559612abab4352b069/.tmp/LC80440342016259LGN00_TOAR_NDVI.tiff> created.",
            ""
          ],
          "stdout": ""
        }
      ],
      "process_results": [
        {
          "cells": 59918181.0,
          "coeff_var": 125.4796560716,
          "max": 1.31488464218245,
          "mean": 0.215349514428788,
          "mean_of_abs": 0.272685223860196,
          "min": -1.35084534300324,
          "n": 41612094.0,
          "name": "LC80440342016259LGN00_TOAR_NDVI",
          "null_cells": 18306087.0,
          "range": 2.6657299851857,
          "stddev": 0.270219830057103,
          "sum": 8961144.23726506,
          "variance": 0.0730187565560894
        }
      ],
      "progress": {
        "num_of_steps": 35,
        "step": 34
      },
      "resource_id": "resource_id-6282c634-42e1-417c-a092-c9b21c3283cc",
      "status": "finished",
      "time_delta": 415.2818741798401,
      "timestamp": 1527679378.31516,
      "urls": {
        "resources": [
          f"http://localhost:5000{URL_PREFIX}/resource/superadmin/resource_id-6282c634-42e1-417c-a092-c9b21c3283cc/tmp80apvh0h.png",
          f"http://localhost:5000{URL_PREFIX}/resource/superadmin/resource_id-6282c634-42e1-417c-a092-c9b21c3283cc/LC80440342016259LGN00_TOAR_NDVI.tiff"
        ],
        "status": f"http://localhost:5000{URL_PREFIX}/resources/superadmin/resource_id-6282c634-42e1-417c-a092-c9b21c3283cc"
      },
      "user_id": "superadmin"
    }

    # required.append("process_results")


def extract_sensor_id_from_scene_id(scene_id):
    """Extract the sensor id from a Landsat scene id

    Args:
        scene_id (str): The landsat scene id

    Returns:
        (str)
        The sencor id

    """
    return "%s0%s" % (scene_id[0:2], scene_id[2:3])


def landsat_scene_id_to_google_url(landsat_scene_id, suffix):
    """Convert a landsat scene id into the public google download URL for the required file

    Args:
        landsat_scene_id (str): The Landsat scene id
        suffix (str): The suffix of the file to create the url for,  i.e.: "_B1.TIF" or "_MTL.txt"
    Returns:
        (str)
        The URL to the scene file
    """
    # URL example
    # https://storage.googleapis.com/gcp-public-data-landsat/LT04/PRE/006/016/LT40060161989006XXX02/LT40060161989006XXX02_MTL.txt

    # Create the download URL components from the Landsat scene id
    landsat_sensor_id = extract_sensor_id_from_scene_id(landsat_scene_id)
    path = landsat_scene_id[3:6]
    row = landsat_scene_id[6:9]

    url = "https://storage.googleapis.com/gcp-public-data-landsat/%s/PRE/%s/%s/%s/%s%s" % (landsat_sensor_id,
                                                                                           path, row,
                                                                                           landsat_scene_id,
                                                                                           landsat_scene_id,
                                                                                           suffix)
    return url


class AsyncEphemeralLandsatProcessingResource(ResourceBase):
    """
    This class represents a resource that runs asynchronous processing tasks
    to download and process Landsat TM satellite images in an ephemeral GRASS location
    """

    decorators = [log_api_call, auth.login_required]

    def __init__(self):
        ResourceBase.__init__(self)
        self.response_model_class = LandsatNDVIResponseModel

    @swagger.doc({
        'tags': ['Satellite Image Algorithms'],
        'description': 'Vegetation index computation from an atmospherically corrected Landsat scene. '
                       'The Landsat scene is located in the google cloud storage. '
                       'The processing is as follows: A user specific Landsat scene (LT4, LT5, LE7 and LC8) '
                       'will be download and imported into an ephemeral database. Then atmospheric correction '
                       'will be performed, with either TOAR or DOS4, depending on the users choice. The user specific '
                       'vegetation index will be computed based on the TOAR or DOS4 data. The result of '
                       'the computation is available as gzipped Geotiff file. In addition, '
                       'the univariate statistic will be computed '
                       'as well as a preview image including a legend. Minimum required user role: user.',
        'parameters': [
            {
                'name': 'landsat_id',
                'description': 'The id of a Landsat scene only with sensors: '
                               'LT04, LT05, LE07, LC08',
                'required': True,
                'in': 'path',
                'type': 'string',
                'default': "LT41970251990147XXX03"
            },
            {
                'name': 'atcor_method',
                'description': 'The method for atmospheric correction',
                'required': True,
                'in': 'path',
                'type': 'string',
                'enum': ["TOAR", "DOS1", "DOS4"],
                'default': 'DOS4'

            },
            {
                'name': 'processing_method',
                'description': 'The method that should be used to compute the vegetation index',
                'required': True,
                'in': 'path',
                'type': 'string',
                'enum': ["NDVI", "ARVI", "DVI", "EVI", "EVI2", "GVI", "GARI",
                         "GEMI", "IPVI", "PVI", "SR", "VARI", "WDVI"],
                'default': 'NDVI'
            }
        ],
        'responses': {
            '200': {
                'description': 'This response includes all created resources '
                               'as URL as well as the processing log and other metadata.',
                'schema': LandsatNDVIResponseModel
            },
            '400': {
                'description': 'The error message and a detailed log why NDVI processing of '
                               'a Landsat scene did not succeeded',
                'schema': ProcessingErrorResponseModel
            }
        }
    })
    def post(self, landsat_id, atcor_method, processing_method):
        """Vegetation index computation from an atmospherically corrected Landsat scene.

        This method will download a single Landsat scene with all bands,
        create a temporary GRASS location and imports the data into it. Then it will
        apply a TOAR or DOS4/1 atmospheric correction, depending on the users choice.
        The imported scenes are then processed via i.vi. The result is analyzed with r.univar
        and rendered via d.rast and d.legend. The preview image and the resulting ndvi raster map
        are stored in the download location.
        As download location are available:
            - local node storage
            - NFS/GlusterFS storage
            - Amazaon S3 storage
            - Google Cloud Storage

        """
        supported_sensors = ["LT04", "LT05", "LE07", "LC08"]
        supported_atcor = ["TOAR", "DOS1", "DOS4"]
        supported_methods = ["NDVI", "ARVI", "DVI", "EVI", "EVI2", "GVI", "GARI",
                             "GEMI", "IPVI", "PVI", "SR", "VARI", "WDVI"]
        sensor_id = extract_sensor_id_from_scene_id(landsat_id)
        if sensor_id not in supported_sensors:
            return self.get_error_response(message="Wrong scene name. "
                                                   "Available sensors are: %s" % ",".join(supported_sensors))

        if atcor_method not in supported_atcor:
            return self.get_error_response(message="Wrong atmospheric correction name. "
                                                   "Available atmospheric corrections are: %s" % ",".join(
                supported_atcor))

        if processing_method not in supported_methods:
            return self.get_error_response(message="Wrong processing method name. "
                                                   "Available methods are: %s" % ",".join(supported_methods))

        # Preprocess the post call
        rdc = self.preprocess(has_json=False, location_name="Landsat")
        rdc.set_user_data((landsat_id, atcor_method, processing_method))
        # rdc.set_storage_model_to_gcs()

        # RedisQueue approach
        enqueue_job(self.job_timeout, start_job, rdc)
        # http_code, data = self.wait_until_finish(0.5)
        html_code, response_model = pickle.loads(self.response_data)
        return make_response(jsonify(response_model), html_code)


def start_job(*args):
    processing = EphemeralLandsatProcessing(*args)
    processing.run()


class EphemeralLandsatProcessing(EphemeralProcessingWithExport):
    """
    """

    def __init__(self, rdc):
        """
        Setup the variables of this class

        Args:
            rdc (ResourceDataContainer): The data container that contains all required variables for processing

        """
        EphemeralProcessingWithExport.__init__(self, rdc)

        self.landsat_scene_id, self.atcor_method, self.processing_method = self.rdc.user_data
        self.landsat_sensor_id = extract_sensor_id_from_scene_id(self.landsat_scene_id)
        self.landsat_band_file_list = []
        self.user_download_cache_path = os.path.join(self.config.DOWNLOAD_CACHE, self.user_id)
        self.raster_result_list = []  # The raster layer names which must be exported, stats computed
        # and preview image created
        self.module_results = []  # A list of r.univar output classes for each vegetation index
        self.response_model_class = LandsatNDVIResponseModel  # The class that is used to create the response

    def _create_temp_database(self, mapsets=[]):
        """Create a temporary gis database and location with a PERMANENT mapset for processing

        Raises:
            This function raises AsyncProcessError in case of an error.

        """
        if not self.landsat_band_file_list:
            raise AsyncProcessError("Unable to create a temporary GIS database, no data is available")

        try:
            geofile = self.landsat_band_file_list[0]
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
                        executable="python3",
                        executable_params=executable_params)

            # Create the GRASS location, this will create the location adn mapset paths
            self._run_process(p)
        except Exception as e:
            raise AsyncProcessError("Unable to create a temporary GIS database and location at <%s>"
                                    ", Exception: %s" % (os.path.join(self.temp_grass_data_base,
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
        univar_command["1"] = {"module": "r.univar",
                               "inputs": {"map": raster_name},
                               "outputs": {"output": {"name": result_file}},
                               "flags": "g"}

        self.request_data = univar_command
        process_list = self._validate_process_chain(skip_permission_check=True)
        self._execute_process_list(process_list=process_list)

        result_list = open(result_file, "r").readlines()
        results = {"name": raster_name}

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

        pc = {}
        pc["1"] = {"module": "d.rast", "inputs": {"map": raster_name},
                   "flags": "n"}

        pc["2"] = {"module": "d.legend", "inputs": {"raster": raster_name,
                                                    "at": "8,92,0,7"},
                   "flags": "n"}

        self.request_data = pc

        # Run the selected modules
        process_list = self._validate_process_chain(skip_permission_check=True)
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
            export_dict = {"name": raster_name,
                           "export": {"format": "GTiff",
                                      "type": "raster"}}
            # Add the raster layer to the export list
            self.resource_export_list.append(export_dict)

        self._update_num_of_steps(len(raster_result_list))

        # Export all resources and generate the finish response
        self._export_resources(use_raster_region=True)

    def _execute(self):
        """Overwrite this function in subclasses

            - Setup user credentials and working paths
            - Create the resource directory
            - Download and store the landsat scene files
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
        process_lib = LandsatProcessing(config=self.config,
                                        temp_file_path=self.temp_file_path,
                                        scene_id=self.landsat_scene_id,
                                        download_cache=self.user_download_cache_path,
                                        message_logger=self.message_logger,
                                        send_resource_update=self._send_resource_update)
        # Generate the download, import and processing command lists
        download_pl, file_infos = process_lib.get_download_process_list()
        self._update_num_of_steps(len(download_pl))
        import_pl = process_lib.get_import_process_list()
        self._update_num_of_steps(len(import_pl))
        toar_pl = process_lib.get_i_landsat_toar_process_list(self.atcor_method)
        self._update_num_of_steps(len(toar_pl))
        ivi_pl = process_lib.get_i_vi_process_list(atcor_method=self.atcor_method,
                                                   processing_method=self.processing_method)
        self._update_num_of_steps(len(ivi_pl))

        # Download all bands from the scene
        if download_pl:
            self._execute_process_list(download_pl)
        self.landsat_band_file_list = process_lib.file_list

        self._create_temporary_grass_environment(source_mapset_name="PERMANENT")

        # Run the import, TOAR and i.vi
        self._execute_process_list(import_pl)
        self._execute_process_list(toar_pl)
        self._execute_process_list(ivi_pl)
        # The ndvi result is an internal variable of the landsat process library
        self.raster_result_list.append(process_lib.ndvi_name)

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

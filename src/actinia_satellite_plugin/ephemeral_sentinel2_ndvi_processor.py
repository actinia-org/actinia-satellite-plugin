# -*- coding: utf-8 -*-
"""
Asynchronous computation in specific temporary generated mapsets
with export of required map layers.
"""
import pickle
import os
import requests
import shutil
import tempfile
from copy import deepcopy
from flask import jsonify, make_response
from flask_restful_swagger_2 import swagger, Schema

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

__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__      = "soerengebbert@googlemail.com"


class SentinelNDVIResponseModel(ProcessingResponseModel):
    """The response content that is returned by the POST request
    """
    type = 'object'
    properties =  deepcopy(ProcessingResponseModel.properties)
    properties["process_results"] = {}
    properties["process_results"]["type"] = "array"
    properties["process_results"]["items"] = UnivarResultModel
    required =  deepcopy(ProcessingResponseModel.required)
    example = {
      "accept_datetime": "2017-05-23 23:07:03.594759",
      "accept_timestamp": 1495573623.594755,
      "api_info": {
        "endpoint": "asyncephemeralsentinel2processingresource",
        "method": "POST",
        "path": "/sentinel2_process/ndvi/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
        "request_url": "http://localhost/sentinel2_process/ndvi/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138"
      },
      "datetime": "2017-05-23 23:07:26.549770",
      "http_code": 200,
      "message": "Processing successfully finished",
      "process_log": [
        {
          "executable": "/usr/local/bin/grass73",
          "parameter": [
            "-e",
            "-c",
            "/tmp/download_cache/admin/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08",
            "/tmp/gisdbase_5607427e55ab4a23ad7f0ddef4c82ed2/sentinel2"
          ],
          "return_code": 0,
          "stderr": [
            "Creating new GRASS GIS location/mapset...",
            "Cleaning up temporary files...",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "v.import",
          "parameter": [
            "input=/tmp/download_cache/admin/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138.gml",
            "output=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
            "--qq"
          ],
          "return_code": 0,
          "stderr": [
            "WARNING: Width for column fid set to 255 (was not specified by OGR), some strings may be truncated!",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "v.timestamp",
          "parameter": [
            "date=12 feb 2017 10:41:38",
            "map=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/tmp/download_cache/admin/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08",
            "output=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped"
          ],
          "return_code": 0,
          "stderr": [
            "Importing raster map <S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped>...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "align=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped",
            "vector=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
            "-g"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=31\nn=4900030\ns=4858620\nw=699960\ne=710330\nnsres=10\newres=10\nrows=4141\ncols=1037\ncells=4294217\n"
        },
        {
          "executable": "r.mask",
          "parameter": [
            "vector=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138"
          ],
          "return_code": 0,
          "stderr": [
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
            "expression=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08 = float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped)"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.timestamp",
          "parameter": [
            "date=12 feb 2017 10:41:38",
            "map=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.remove",
          "parameter": [
            "type=raster",
            "name=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped",
            "-f"
          ],
          "return_code": 0,
          "stderr": [
            "Removing raster <S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08_uncropped>",
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
          "stderr": [
            "Raster MASK removed",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.import",
          "parameter": [
            "input=/tmp/download_cache/admin/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04",
            "output=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped"
          ],
          "return_code": 0,
          "stderr": [
            "Importing raster map <S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped>...",
            "0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96..99..100",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.region",
          "parameter": [
            "align=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped",
            "vector=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
            "-g"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=31\nn=4900030\ns=4858620\nw=699960\ne=710330\nnsres=10\newres=10\nrows=4141\ncols=1037\ncells=4294217\n"
        },
        {
          "executable": "r.mask",
          "parameter": [
            "vector=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138"
          ],
          "return_code": 0,
          "stderr": [
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
            "expression=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04 = float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped)"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.timestamp",
          "parameter": [
            "date=12 feb 2017 10:41:38",
            "map=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "g.remove",
          "parameter": [
            "type=raster",
            "name=S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped",
            "-f"
          ],
          "return_code": 0,
          "stderr": [
            "Removing raster <S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04_uncropped>",
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
          "stderr": [
            "Raster MASK removed",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "r.mapcalc",
          "parameter": [
            "expression=ndvi = (float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08) - float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04))/(float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B08) + float(S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_B04))"
          ],
          "return_code": 0,
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
            "output=/tmp/gisdbase_5607427e55ab4a23ad7f0ddef4c82ed2/.tmp/tmpA8mqEm.univar",
            "-g"
          ],
          "return_code": 0,
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
          "stderr": [
            ""
          ],
          "stdout": "projection=1\nzone=31\nn=4900030\ns=4858620\nw=699960\ne=710330\nnsres=10\newres=10\nrows=4141\ncols=1037\ncells=4294217\n"
        },
        {
          "executable": "r.out.gdal",
          "parameter": [
            "-f",
            "input=ndvi",
            "format=GTiff",
            "output=/tmp/gisdbase_5607427e55ab4a23ad7f0ddef4c82ed2/.tmp/ndvi.tiff"
          ],
          "return_code": 0,
          "stderr": [
            "Checking GDAL data type and nodata value...",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "Using GDAL data type <Float32>",
            "Input raster map contains cells with NULL-value (no-data). The value -nan will be used to represent no-data values in the input map. You can specify a nodata value with the nodata option.",
            "Exporting raster data to GTiff format...",
            "ERROR 6: SetColorTable() only supported for Byte or UInt16 bands in TIFF format.",
            "2..5..8..11..14..17..20..23..26..29..32..35..38..41..44..47..50..53..56..59..62..65..68..71..74..77..80..83..86..89..92..95..98..100",
            "r.out.gdal complete. File </tmp/gisdbase_5607427e55ab4a23ad7f0ddef4c82ed2/.tmp/ndvi.tiff> created.",
            ""
          ],
          "stdout": ""
        },
        {
          "executable": "/bin/gzip",
          "parameter": [
            "/tmp/gisdbase_5607427e55ab4a23ad7f0ddef4c82ed2/.tmp/ndvi.tiff"
          ],
          "return_code": 0,
          "stderr": [
            ""
          ],
          "stdout": ""
        }
      ],
      "process_results": [
        {
          "cells": 4294217.0,
          "coeff_var": 59.5970846784418,
          "max": 0.6962730884552,
          "mean": 0.227711493710824,
          "mean_of_abs": 0.231561380576833,
          "min": -1.0,
          "n": 2140249.0,
          "name": "ndvi",
          "null_cells": 2153968.0,
          "range": 1.6962730884552,
          "stddev": 0.135709411729384,
          "sum": 487359.296703097,
          "variance": 0.0184170444319356
        }
      ],
      "progress": {
        "num_of_steps": 27,
        "step": 26
      },
      "resource_id": "resource_id-01b1a76a-3dae-4e46-88f0-f820cfb3b264",
      "status": "finished",
      "timestamp": 1495573646.549768,
      "urls": {
        "resources": [
          "http://localhost/resource/admin/resource_id-01b1a76a-3dae-4e46-88f0-f820cfb3b264/tmpvYOaAe.png",
          "http://localhost/resource/admin/resource_id-01b1a76a-3dae-4e46-88f0-f820cfb3b264/ndvi.tiff.gz"
        ],
        "status": "http://localhost/status/admin/resource_id-01b1a76a-3dae-4e46-88f0-f820cfb3b264"
      },
      "user_id": "admin"
    }

    # required.append("process_results")


SWAGGER_DOC = {
    'tags': ['satellite image algorithms'],
    'description': 'NDVI computation of a Sentinel 2A '
                   'scene that is downloaded from the google cloud storage. '
                   'The processing is as follows: A user specific Sentinel 2A scene (Bands 04 and 08)'
                   'will be download and imported into a temporary GRASS location. '
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
            'schema':ProcessingResponseModel
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
        """NDVI computation of an arbitrary Sentinel 2A scene
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
            executable_params.append("-e")
            executable_params.append("-c")
            executable_params.append(geofile)
            executable_params.append(os.path.join(self.temp_grass_data_base, self.location_name))

            self.message_logger.info("%s %s"%(self.config.GRASS_GIS_START_SCRIPT, executable_params))

            self._update_num_of_steps(1)

            p = Process(exec_type="exec",
                             executable=self.config.GRASS_GIS_START_SCRIPT,
                             executable_params=executable_params)

            # Create the GRASS location, this will create the location adn mapset paths
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

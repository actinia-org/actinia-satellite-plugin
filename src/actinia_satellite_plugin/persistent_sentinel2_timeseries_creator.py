# -*- coding: utf-8 -*-
"""
"""
import pickle
import os
from flask import jsonify, make_response
import dateutil.parser as dtparser
from datetime import timedelta
from copy import deepcopy
from flask_restful_swagger_2 import swagger, Schema
from actinia_core.models.response_models import (
    ProcessingResponseModel,
    ProcessingErrorResponseModel,
)
from actinia_core.processing.actinia_processing.ephemeral\
    .persistent_processing import PersistentProcessing
from actinia_core.rest.base.resource_base import ResourceBase
from actinia_core.core.common.kvdb_interface import enqueue_job
from actinia_core.core.common.google_satellite_bigquery_interface import (
    GoogleSatelliteBigQueryInterface,
)
from actinia_core.core.common.sentinel_processing_library import (
    Sentinel2Processing,
)
from actinia_core.core.common.exceptions import AsyncProcessError

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"


class Sentinel2ASceneListModel(Schema):
    """
    This schema defines the JSON input of the sentinel time series creator
    resource
    """

    type = "object"
    properties = {
        "bands": {
            "type": "array",
            "items": {"type": "string"},
            "description": "A list of band names that should be downloaded "
                           "and imported for each Sentinel-2 scene."
            'Available are the following band names: "B01", "B02", "B03", '
            '"B04", "B05", "B06", "B07", "B08", "B8A", "B09" "B10", "B11", '
            '"B12"',
        },
        "strds": {
            "type": "array",
            "items": {"type": "string"},
            "description": "The names of the new space-time raster datasets,"
                           " one for each band",
        },
        "product_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "A list of Sentinel-2 scene names that should be "
                           "downloaded and imported",
        },
    }
    example = {
        "bands": ["B04", "B08"],
        "strds": ["Sentinel_B04", "Sentinel_b08"],
        "product_ids": [
            "S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
            "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_20170227T095613",
            "S2A_MSIL1C_20170202T104241_N0204_R008_T32UNE_20170202T104236",
        ],
    }

    required = ["bands", "strds", "product_ids"]


SCHEMA_DOC = {
    "tags": ["Satellite Image Algorithms"],
    "description": "Download and import Sentinel2A scenes into a new mapset "
    "and create a space-time raster dataset "
    "for each imported band. "
    "The resulting data will be located in a persistent user database. "
    "The project name is part of the path and must exist. The mapset will "
    "be created while importing and should not already exist in the project. "
    "The names of the Sentinel-2 scenes and the band names that should"
    " be downloaded must be specified in the HTTP body as"
    " application/json content. In addition, the names of the "
    "STRDS that should manage the sentinel scenes must"
    " be provided in the application/json content. For each band a separate "
    "STRDS name must be provided. This call "
    "is performed asynchronously. The provided resource URL must be pulled "
    "to receive the status of the import. The data is available in the "
    "provided project/mapset, after the download and import finished. "
    "Minimum required user role: user.",
    "consumes": ["application/json"],
    "parameters": [
        {
            "name": "project_name",
            "description": "The project name to import the Sentinel2A scenes "
                           "in",
            "required": True,
            "in": "path",
            "type": "string",
        },
        {
            "name": "mapset_name",
            "description": "The name of the mapset to import the Sentinel2A "
                           "scenes in",
            "required": True,
            "in": "path",
            "type": "string",
        },
        {
            "name": "tiles",
            "description": "The list of Sentinel-2 scenes, the band names and "
                           "the target STRDS names",
            "required": True,
            "in": "body",
            "schema": Sentinel2ASceneListModel,
        },
    ],
    "responses": {
        "200": {
            "description": "The result of the Sentinel-2 time series import",
            "schema": ProcessingResponseModel,
        },
        "400": {
            "description": "The error message and a detailed log why Sentinel "
            "2A time series import did not succeeded",
            "schema": ProcessingErrorResponseModel,
        },
    },
}


class AsyncSentinel2TimeSeriesCreatorResource(ResourceBase):
    def __init__(self):
        ResourceBase.__init__(self)

    @swagger.doc(deepcopy(SCHEMA_DOC))
    def post(self, project_name, mapset_name):
        """
        Download and import Sentinel2A scenes into a new mapset and create
        a space-time raster dataset for each imported band.

        Args:
            project_name (str): The name of the project
            target_mapset_name (str): The name of the mapset that should be
                                      created

        Process arguments must be provided as JSON document in the POST request

              {"bands":["B04","B08"],
               "strds":["Sentinel_B04", "Sentinel_b08"],
               "product_ids":["S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_"
                              "20170212T104138",
                              "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_"
                              "20170227T095613",
                              "S2A_MSIL1C_20170202T104241_N0204_R008_T32UNE_"
                              "20170202T104236"]}

        Returns:
            flask.Response:
            The HTTP status and a JSON document that includes the
            status URL of the task that must be polled for updates.

        Example::

            {
              "HTTP code": 200,
              "Messages": "Resource accepted",
              "Resource id": "resource_id-985164c9-1db9-49cf-b2c4-"
                             "3e8e48500e31",
              "Status": "accepted",
              "URLs": {
                "Resources": [],
                "Status": "http://104.155.60.87/status/soeren/resource_id-"
                          "985164c9-1db9-49cf-b2c4-3e8e48500e31"
              },
              "User id": "soeren"
            }


        """
        # Preprocess the post call
        rdc = self.preprocess(
            has_json=True, project_name=project_name, mapset_name=mapset_name
        )

        # KvdbQueue approach
        enqueue_job(self.job_timeout, start_job, rdc)

        html_code, response_model = pickle.loads(self.response_data)
        return make_response(jsonify(response_model), html_code)


def start_job(*args):
    processing = AsyncSentinel2TimeSeriesCreator(*args)
    processing.run()


class AsyncSentinel2TimeSeriesCreator(PersistentProcessing):
    """
    Create a space time raster dataset from all provided product_ids for
    each Sentinel2A band in a new mapset.

    The Sentiel2A scenes are downloaded , imported and pre-processed before
    they are registered in the band specific space time datasets.
    """

    def __init__(self, rdc):
        """Constructor

        Args:
            rdc (ResourceDataContainer): The data container that contains all
                                         required variables for processing

        """
        PersistentProcessing.__init__(self, rdc)

        # This works only if the mapset snot already exists
        self.temp_mapset_name = self.mapset_name

        self.query_interface = GoogleSatelliteBigQueryInterface(self.config)
        self.product_ids = self.rdc.request_data["product_ids"]
        self.strds_ids = self.rdc.request_data["strds"]
        self.required_bands = self.rdc.request_data["bands"]
        self.user_download_cache_path = os.path.join(
            self.config.DOWNLOAD_CACHE, self.user_id
        )
        self.query_result = None

    def _prepare_sentinel2_download(self):
        """
        Check the download cache if the file already exists, to avoid redundant
        downloads.
        The downloaded files will be stored in a temporary directory.
        After the download of all files completes, the downloaded files will
        be moved to the download cache. This avoids broken files in case a
        download was interrupted or stopped by termination.

        """
        # Create the download cache directory if it does not exists
        if os.path.exists(self.config.DOWNLOAD_CACHE):
            pass
        else:
            os.mkdir(self.config.DOWNLOAD_CACHE)

        # Create the user specific download cache directory to put the
        # downloaded files into it
        if os.path.exists(self.user_download_cache_path):
            pass
        else:
            os.mkdir(self.user_download_cache_path)

        # Switch into the tempfile directory
        os.chdir(self.temp_file_path)

        # We have to set the home directory to create the grass project
        os.putenv("HOME", "/tmp")

        self._send_resource_update("Sending Google BigQuery request.")

        try:
            self.query_result = self.query_interface.get_sentinel_urls(
                self.product_ids, self.required_bands
            )
        except Exception as e:
            raise AsyncProcessError(
                "Error in querying Sentinel-2 product <%s> "
                "in Google BigQuery Sentinel-2 database. "
                "Error: %s" % (self.product_ids, str(e))
            )

        if not self.query_result:
            raise AsyncProcessError(
                "Unable to find Sentinel-2 product <%s> "
                "in Google BigQuery Sentinel-2 database" % self.product_ids
            )

    def _import_sentinel2_scenes(self):
        """
        Import all found Sentinel2 scenes with their bands and create the
        space time raster datasets to register the maps in.

        Raises:
            AsyncProcessError: In case something went wrong

        """

        all_commands = []
        counter = 0

        # Use only the product ids that were found in the big query
        for product_id in self.query_result:

            process_lib = Sentinel2Processing(
                self.config,
                product_id,
                self.query_result,
                self.required_bands,
                self.temp_file_path,
                self.user_download_cache_path,
                self._send_resource_update,
                self.message_logger,
            )

            (
                download_commands,
                self.sentinel2_band_file_list,
            ) = process_lib.get_sentinel2_download_process_list()

            # Download the sentinel scene if not in the download cache
            if download_commands:
                all_commands.extend(download_commands)

            # Import and prepare the sentinel scenes
            import_commands = process_lib.get_sentinel2_import_process_list()
            all_commands.extend(import_commands)

        self._update_num_of_steps(len(all_commands))
        self._execute_process_list(process_list=all_commands)

        # IMPORTANT:
        # The registration must be performed in the temporary mapset with the
        # same name as the target mapset,
        # since the temporal database will contain the mapset name.

        register_commands = {}

        result_dict = {}

        for i in range(len(self.required_bands)):
            band = self.required_bands[i]
            strds = self.strds_ids[i]

            result_dict[band] = []

            create_strds = {
                "module": "t.create",
                "inputs": {
                    "output": strds,
                    "title": "Sentinel2A time series for band %s" % band,
                    "description": "Sentinel2A time series for band %s" % band,
                    "temporaltype": "absolute",
                    "type": "strds",
                },
            }

            register_commands[str(counter)] = create_strds
            counter += 1

            # Create the input file
            map_list_file_name = os.path.join(
                self.user_download_cache_path, strds
            )
            map_list_file = open(map_list_file_name, "w")

            # Use only the product ids that were found in the big query
            for product_id in self.query_result:

                # We need to create a time interval, otherwise the temporal
                # algebra will not work :/
                start_time = dtparser.parse(
                    self.query_result[product_id]["timestamp"].split(".")[0]
                )
                end_time = start_time + timedelta(seconds=1)

                result_dict[band].append(
                    [
                        self.query_result[product_id][band]["file"],
                        str(start_time),
                        str(end_time),
                    ]
                )

                map_list_file.write(
                    "%s|%s|%s\n"
                    % (
                        self.query_result[product_id][band]["file"],
                        str(start_time),
                        str(end_time),
                    )
                )
            map_list_file.close()

            register_maps = {
                "module": "t.register",
                "inputs": {
                    "input": strds,
                    "file": map_list_file_name,
                    "type": "raster",
                },
            }

            register_commands[str(counter)] = register_maps
            counter += 1

        process_list = self._validate_process_chain(
            process_chain=register_commands, skip_permission_check=True
        )
        self._execute_process_list(process_list=process_list)

        self.module_results = result_dict

    def _execute(self):

        # Setup the user credentials and logger
        self._setup()

        if len(self.required_bands) != len(self.strds_ids):
            raise AsyncProcessError(
                "The number of bands and the number of strds must be equal"
            )

        for band in self.required_bands:
            if self.required_bands.count(band) > 2:
                raise AsyncProcessError("The band names must be unique")

        for strds in self.strds_ids:
            if self.strds_ids.count(strds) > 2:
                raise AsyncProcessError("The strds names must be unique")

        # Check and lock the target and temp mapsets
        self._check_lock_target_mapset()

        if self.target_mapset_exists is True:
            raise AsyncProcessError(
                "Sentinel time series can only be create in a new mapset. "
                "Mapset <%s> already exists." % self.target_mapset_name
            )

        # Init GRASS environment and create the temporary mapset with the same
        # name as the target mapset
        # This is required to register the raster maps in the temporary
        # directory, but use them in persistent directory

        # Create the temp database and link the
        # required mapsets into it
        self._create_temp_database(self.required_mapsets)

        # Initialize the GRASS environment and switch into PERMANENT
        # mapset, which is always linked
        self._create_grass_environment(
            grass_data_base=self.temp_grass_data_base, mapset_name="PERMANENT"
        )

        # Create the temporary mapset and switch into it
        self._create_temporary_mapset(temp_mapset_name=self.target_mapset_name)

        # Setup the download cache and query the BigQuery database of google
        # for product_ids
        self._prepare_sentinel2_download()

        # Check if all product ids were found
        missing_product_ids = []
        for product_id in self.product_ids:
            if product_id not in self.query_result:
                missing_product_ids.append(product_id)

        # Abort if a single scene is missing
        if len(missing_product_ids) > 0:
            raise AsyncProcessError(
                "Unable to find product ids <%s> in the "
                "Google BigQuery database" % str(missing_product_ids)
            )

        self._import_sentinel2_scenes()

        # Copy local mapset to original project
        self._copy_merge_tmp_mapset_to_target_mapset()

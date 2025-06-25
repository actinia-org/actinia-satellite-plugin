# -*- coding: utf-8 -*-
"""
Asynchronous merging of several mapsets into a single one
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
from actinia_processing_lib.persistent_processing import PersistentProcessing
from actinia_rest_lib.resource_base import ResourceBase
from actinia_core.core.common.kvdb_interface import enqueue_job
from actinia_core.core.common.google_satellite_bigquery_interface import (
    GoogleSatelliteBigQueryInterface,
)
from actinia_core.core.common.landsat_processing_library import (
    LandsatProcessing,
    SCENE_BANDS,
    extract_sensor_id_from_scene_id,
    RASTER_SUFFIXES,
)
from actinia_processing_lib.exceptions import AsyncProcessError

__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Sören Gebbert"
__email__ = "soerengebbert@googlemail.com"


class LandsatSceneListModel(Schema):
    """
    This schema defines the JSON input of the Landsat time series creator
    resource
    """

    type = "object"
    properties = {
        "atcor_method": {
            "description": "The method for atmospheric correction",
            "type": "string",
            "enum": ["TOAR", "DOS1", "DOS4"],
        },
        "strds": {
            "type": "string",
            "description": "The basename of the new space-time raster datasets"
            " (strds). "
            "One for each band will be created and the basename will be "
            "extended by a band specific suffix.",
        },
        "scene_ids": {
            "type": "array",
            "items": {"type": "string"},
            "description": "A list of Landsat scene names that should be"
            " downloaded and imported. Only scenes from a "
            "specific satelleite can be imported and represented as strds. "
            "Mixing of different satellites is not permitted. All bands will "
            "be imported "
            "and atmospherically corrected",
        },
    }
    example = {
        "strds": "Landsat_4",
        "atcor_method": "TOAR",
        "scene_ids": [
            "LT41970251990147XXX00",
            "LT41970251990147XXX01",
            "LT41970251990147XXX02",
            "LT41970251990147XXX03",
        ],
    }

    required = ["strds", "scene_ids", "atcor_method"]


SCHEMA_DOC = {
    "tags": ["Satellite Image Algorithms"],
    "description": "Download and import Landsat scenes into a new mapset and "
    "create a space-time raster dataset "
    "for each imported band. "
    "The resulting data will be located in a persistent user database. "
    "The project name is part of the path and must exist. The mapset will "
    "be created while importing and should not already exist in the project. "
    "The names of the"
    "Landsat scenes that should be downloaded must be specified "
    "in the HTTP body as application/json content. In addition, the basename"
    " of the STRDS that should manage the Landsat scenes must"
    " be provided in the application/json content. For each band a separate "
    "strds will be cerated and the STRDS base name will be extended with the "
    "band number. This call is performed asynchronously. The provided resource"
    " URL must be pulled to receive the status of the import. The data is "
    "available in the provided project/mapset, after the download and import"
    " finished. Minimum required user role: user.",
    "consumes": ["application/json"],
    "parameters": [
        {
            "name": "project_name",
            "description": "The project name to import the Landsat scenes in",
            "required": True,
            "in": "path",
            "type": "string",
        },
        {
            "name": "mapset_name",
            "description": "The name of the mapset to import the Landsat "
                           "scenes in",
            "required": True,
            "in": "path",
            "type": "string",
        },
        {
            "name": "tiles",
            "description": "The list of Landsat scenes, the band names and "
                           "the target STRDS names",
            "required": True,
            "in": "body",
            "schema": LandsatSceneListModel,
        },
    ],
    "responses": {
        "200": {
            "description": "The result of the Landsat time series import",
            "schema": ProcessingResponseModel,
        },
        "400": {
            "description": "The error message and a detailed log why Landsat "
            "time series import did not succeeded",
            "schema": ProcessingErrorResponseModel,
        },
    },
}


class AsyncLandsatTimeSeriesCreatorResource(ResourceBase):
    def __init__(self):
        ResourceBase.__init__(self)

    @swagger.doc(deepcopy(SCHEMA_DOC))
    def post(self, project_name, mapset_name):
        """
        Download and import Landsat scenes into a new mapset and create a
        space time dataset for each imported band.

        Args:
            project_name (str): The name of the project
            target_mapset_name (str): The name of the mapset that should be
                                      created

        Process arguments must be provided as JSON document in the POST request

              {"strds":"Landsat_4_1983_09_01_01_30_00",
               "atcor_method": "TOAR",
               "scene_ids":["LM41130251983244HAJ00",
                            "LM41130271983244FFF03",
                            "LM41130261983244FFF03",
                            "LM41130241983244HAJ00"]}
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
    processing = LandsatTimeSeriesCreator(*args)
    processing.run()


class LandsatTimeSeriesCreator(PersistentProcessing):
    """
    Create a space time raster dataset from all provided scene_ids for each
    Sentinel2A band
    in a new mapset.

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
        self.scene_ids = self.rdc.request_data["scene_ids"]
        self.strds_basename = self.rdc.request_data["strds"]
        self.atcor_method = self.rdc.request_data["atcor_method"]
        self.user_download_cache_path = os.path.join(
            self.config.DOWNLOAD_CACHE, self.user_id
        )
        self.query_result = None
        self.required_bands = None
        self.sensor_id = None

    def _prepare_download(self):
        """
        Check the download cache if the file already exists, to avoid redundant
        downloads.
        The downloaded files will be stored in a temporary directory.
        After the download of all files completes, the downloaded files will
        be moved to the download cache. This avoids broken
        files in case a download was interrupted or stopped by termination.

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

        sensor_ids = {}
        for scene_id in self.scene_ids:

            # Check if the the list of scenes contains scenes from different
            # satellites
            self.sensor_id = extract_sensor_id_from_scene_id(scene_id=scene_id)
            sensor_ids[self.sensor_id] = self.sensor_id

            if len(sensor_ids) > 2:
                raise AsyncProcessError(
                    "Different satellites are in the list of scenes to be "
                    "imported. Only scenes from a single satellite an be "
                    "imported at once."
                )

        # All bands are imported, except the MTL file
        self.required_bands = SCENE_BANDS[self.sensor_id][0:-1]

        self._send_resource_update("Sending Google BigQuery request.")

        try:
            self.query_result = self.query_interface.get_landsat_urls(
                self.scene_ids, self.required_bands
            )
        except Exception as e:
            raise AsyncProcessError(
                "Error in querying Landsat product <%s> "
                "in Google BigQuery Landsat database. "
                "Error: %s" % (self.scene_ids, str(e))
            )

        if not self.query_result:
            raise AsyncProcessError(
                "Unable to find Landsat product <%s> "
                "in Google BigQuery Landsat database" % self.scene_ids
            )

    def _import_scenes(self):
        """
        Import all found Sentinel2 scenes with their bands and create the
        space time raster datasets to register the maps in.

        Raises:
            AsyncProcessError: In case something went wrong

        """

        all_commands = []
        counter = 0

        for scene_id in self.query_result:

            process_lib = LandsatProcessing(
                config=self.config,
                scene_id=scene_id,
                temp_file_path=self.temp_file_path,
                download_cache=self.user_download_cache_path,
                send_resource_update=self._send_resource_update,
                message_logger=self.message_logger,
            )

            (
                download_commands,
                self.sentinel2_band_file_list,
            ) = process_lib.get_download_process_list()

            # Download the Landsat scene if it is not in the download cache
            if download_commands:
                all_commands.extend(download_commands)

            # Import and atmospheric correction
            import_commands = process_lib.get_import_process_list()
            all_commands.extend(import_commands)
            atcor_method_commands = (
                process_lib.get_i_landsat_toar_process_list(
                    atcor_method=self.atcor_method
                )
            )
            all_commands.extend(atcor_method_commands)

        # Run the commands
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
            strds = self.strds_basename + "_%s" % band

            result_dict[band] = []

            create_strds = {
                "module": "t.create",
                "inputs": {
                    "output": strds,
                    "title": "Landsat time series for band %s" % band,
                    "description": "Landsat time series for band %s" % band,
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
            for scene_id in self.query_result:

                # We need to create a time interval, otherwise the temporal
                # algebra will not work :/
                start_time = dtparser.parse(
                    self.query_result[scene_id]["timestamp"].split(".")[0]
                )
                end_time = start_time + timedelta(seconds=1)

                index = SCENE_BANDS[self.sensor_id].index(band)
                raster_suffix = RASTER_SUFFIXES[self.sensor_id][index]
                map_name = "%s_%s%s" % (
                    scene_id,
                    self.atcor_method,
                    raster_suffix,
                )

                result_dict[band].append(
                    [map_name, str(start_time), str(end_time)]
                )

                map_list_file.write(
                    "%s|%s|%s\n" % (map_name, str(start_time), str(end_time))
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
        # for scene_ids
        self._prepare_download()

        # Check if all product ids were found
        missing_scene_ids = []
        for scene_id in self.scene_ids:
            if scene_id not in self.query_result:
                missing_scene_ids.append(scene_id)

        # Abort if a single scene is missing
        if len(missing_scene_ids) > 0:
            raise AsyncProcessError(
                "Unable to find product ids <%s> in the "
                "Google BigQuery database" % str(missing_scene_ids)
            )

        self._import_scenes()

        # Copy local mapset to original project
        self._copy_merge_tmp_mapset_to_target_mapset()

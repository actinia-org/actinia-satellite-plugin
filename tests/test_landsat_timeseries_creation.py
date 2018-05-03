# -*- coding: utf-8 -*-
import unittest
from pprint import pprint
from flask.json import loads as json_load
from flask.json import dumps as json_dump
try:
    from .test_resource_base import ActiniaResourceTestCaseBase
except:
    from test_resource_base import ActiniaResourceTestCaseBase


__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__      = "soerengebbert@googlemail.com"

# Module change example for r.slope.aspect with g.region adjustment

SCENE_IDS = {"strds":"Landsat_4",
             "atcor_method": "TOAR",
             "scene_ids":["LT41970251990147XXX03"]}

WRONG_SCENE_IDS = {"strds":"Landsat_4",
                   "atcor_method": "TOAR",
                   "scene_ids":["LT41970251990147XXX00",
                                "LT41970251990147XXX01"]}

test_mapsets = ["A"]


class AsyncLandsatTimeSeriesCreationTestCaseAdmin(ActiniaResourceTestCaseBase):
    """test the download and creation of sentinel2 time series in a new mapset

    TODO: Implement error tests for wrong scene ids and atmospheric correction methods
    """

    def check_remove_test_mapsets(self):
        """
        Check and remove test mapsets that have been created in the test-run
        Returns:

        """

        rv = self.server.get('/locations/LL/mapsets',
                             headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        mapsets = json_load(rv.data)["process_results"]

        for mapset in test_mapsets:
            if mapset in mapsets:
                # Unlock mapset for deletion
                rv = self.server.post('/locations/LL/mapsets/%s' % mapset,
                                      headers=self.admin_auth_header)
                pprint(json_load(rv.data))
                # Delete the mapset if it already exists
                rv = self.server.delete('/locations/LL/mapsets/%s' % mapset,
                                        headers=self.admin_auth_header)
                pprint(json_load(rv.data))
                self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
                self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

    def test_1(self):
        """Test the import of two scenes with 2 bands in a new mapset A"""
        self.check_remove_test_mapsets()

        ############################################################################
        rv = self.server.post('/locations/LL/mapsets/A/landsat_import',
                              headers=self.admin_auth_header,
                              data=json_dump(SCENE_IDS),
                              content_type="application/json")

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header)

        rv = self.server.get('/locations/LL/mapsets/A/strds',
                             headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        strds_list = json_load(rv.data)["process_results"]
        self.assertTrue("Landsat_4_B1" in strds_list)
        self.assertTrue("Landsat_4_B2" in strds_list)
        self.assertTrue("Landsat_4_B3" in strds_list)
        self.assertTrue("Landsat_4_B4" in strds_list)
        self.assertTrue("Landsat_4_B5" in strds_list)
        self.assertTrue("Landsat_4_B6" in strds_list)
        self.assertTrue("Landsat_4_B7" in strds_list)

    def test_wrong_scene_ids(self):
        """Test the import of two scenes with 2 bands in a new mapset A"""
        self.check_remove_test_mapsets()

        ############################################################################
        rv = self.server.post('/locations/LL/mapsets/A/landsat_import',
                              headers=self.admin_auth_header,
                              data=json_dump(WRONG_SCENE_IDS),
                              content_type="application/json")

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=400, status="error",
                                       message_check="AsyncProcessError:")

    def test_1_error_mapset_exists(self):
        """PERMANENT mapset exists. hence an error message is expected
        """
        rv = self.server.post('/locations/LL/mapsets/PERMANENT/landsat_import',
                              headers=self.admin_auth_header,
                              data=json_dump(SCENE_IDS),
                              content_type="application/json")

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=400, status="error",
                                       message_check="AsyncProcessError:")


if __name__ == '__main__':
    unittest.main()

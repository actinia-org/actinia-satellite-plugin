# -*- coding: utf-8 -*-
import unittest
from pprint import pprint
from flask.json import loads as json_load
try:
    from .test_resource_base import ActiniaResourceTestCaseBase
except:
    from test_resource_base import ActiniaResourceTestCaseBase


__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__      = "soerengebbert@googlemail.com"


class SentinelProcessingTestCase(ActiniaResourceTestCaseBase):

    def test_ndvi_computation_small(self):

        # Large scene
        # rv = self.server.post('/sentinel2_process/S2A_MSIL1C_20170216T102101_N0204_R065_T32UPV_20170216T102204',
        #                       headers=self.admin_auth_header)
        # Small scene
        rv = self.server.post('/sentinel2_process/ndvi/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=200, status="finished")

    def incative_test_ndvi_computation_small_gcs(self):

        # Large scene
        # rv = self.server.post('/sentinel2_process/S2A_MSIL1C_20170216T102101_N0204_R065_T32UPV_20170216T102204',
        #                       headers=self.admin_auth_header)
        # Small scene
        rv = self.server.post('/sentinel2_process_gcs/ndvi/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=200, status="finished")

    def incative_test_ndvi_computation_big(self):

        # Small scene
        # rv = self.server.post('/sentinel2_process/S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138',
        #                       headers=self.admin_auth_header)
        # Large scene
        rv = self.server.post('/sentinel2_process/ndvi/S2A_MSIL1C_20170216T102101_N0204_R065_T32UPV_20170216T102204',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=200, status="finished")

    def incative_test_computation_error_1(self):
        rv = self.server.post('/sentinel2_process/ndvi/S2A_MSIL1C_20170212T1041_BLABLA_8_T31TGJ_20170212T104138.SAFE',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=400, status="error")

    def incative_test_computation_error_2(self):
        rv = self.server.post('/sentinel2_process_gcs/ndvi/S2A_MSIL1C_20170212T1041_BLABLA_8_T31TGJ_20170212T104138.SAFE',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=400, status="error")

if __name__ == '__main__':
    unittest.main()

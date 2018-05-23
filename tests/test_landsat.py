# -*- coding: utf-8 -*-
import unittest
from pprint import pprint
from flask.json import loads as json_load
try:
    from .test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX
except:
    from test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX


__license__ = "GPLv3"
__author__     = "Sören Gebbert"
__copyright__  = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__      = "soerengebbert@googlemail.com"


class LandsatProcessingTestCase(ActiniaResourceTestCaseBase):

    def test_landsat_computation_DOS4(self):
        rv = self.server.post(URL_PREFIX + '/landsat_process/LT41970251990147XXX03/DOS4/NDVI',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=200, status="finished")

        rv = self.server.get(URL_PREFIX + '/download_cache', headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.assertTrue("used" in json_load(rv.data)["process_results"])
        self.assertTrue("quota" in json_load(rv.data)["process_results"])
        self.assertTrue("free" in json_load(rv.data)["process_results"])
        self.assertTrue("free_percent" in json_load(rv.data)["process_results"])

    def test_landsat_computation_TOAR(self):
        rv = self.server.post(URL_PREFIX + '/landsat_process/LT41970251990147XXX03/TOAR/NDVI',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=200, status="finished")

        rv = self.server.get(URL_PREFIX + '/download_cache', headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.assertTrue("used" in json_load(rv.data)["process_results"])
        self.assertTrue("quota" in json_load(rv.data)["process_results"])
        self.assertTrue("free" in json_load(rv.data)["process_results"])
        self.assertTrue("free_percent" in json_load(rv.data)["process_results"])

    def test_landsat_computation_error_1(self):
        rv = self.server.post(URL_PREFIX + '/landsat_process/LT41970251990147XXX03/POS/NDVI',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 400, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

    def test_landsat_computation_error_2(self):
        rv = self.server.post(URL_PREFIX + '/landsat_process/LT41970251990147XXX03/TOAR/LOLO',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 400, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

    def test_landsat_computation_error_3(self):
        rv = self.server.post(URL_PREFIX + '/landsat_process/LT41970241987299XXX0_NOPE/TOAR/NDVI',
                              headers=self.admin_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header,
                                       http_status=400, status="error")

if __name__ == '__main__':
    unittest.main()

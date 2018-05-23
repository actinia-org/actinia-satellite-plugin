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


class GrassModuleTestCase(ActiniaResourceTestCaseBase):

    def test_landsat_query_time_interval(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?start_time=2001-01-01T00:00:00&end_time=2001-01-01T01:00:00',
                             headers=self.user_auth_header)
        data = json_load(rv.data)
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_sentinel2_query_time_interval(self):

        rv = self.server.get(URL_PREFIX + '/sentinel2_query?start_time=2017-01-01T00:00:00&end_time=2017-01-01T00:30:00',
                             headers=self.user_auth_header)
        # print rv.data
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertEqual(len(data), 204)

    def test_landsat_query_time_interval_lat_lon(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?start_time=2001-01-01T00:00:00&end_time=2001-01-01T01:00:00&lon=154&lat=51',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_landsat_query_scene_id(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?scene_id=LE71010632001001EDC01',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_sentinel2_query_scene_id(self):

        rv = self.server.get(URL_PREFIX + '/sentinel2_query?scene_id=S2B_MSIL1C_20171010T131249_N0205_R081_T26VPR_20171010T131243',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_landsat_query_scene_id_cloud(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?scene_id=LE71010632001001EDC01&cloud_cover=100.0',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_landsat_query_scene_id_cloud_spacecraft(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?scene_id=LE71010632001001EDC01&cloud_cover=100.0&spacecraft_id=LANDSAT_7',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

    def test_z_landsat_query_cloud_spacecraft(self):

        rv = self.server.get(URL_PREFIX + '/landsat_query?start_time=1983-09-01T01:00:00&end_time=1983-09-01T01:20:00&cloud_cover=0.0&spacecraft_id=LANDSAT_4',
                             headers=self.user_auth_header)
        pprint(json_load(rv.data))
        self.assertEqual(rv.status_code, 200, "HTML status code is wrong %i"%rv.status_code)
        self.assertEqual(rv.mimetype, "application/json", "Wrong mimetype %s"%rv.mimetype)

        data = json_load(rv.data)
        self.assertTrue(len(data) >= 1)

if __name__ == '__main__':
    unittest.main()

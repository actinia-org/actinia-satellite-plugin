# -*- coding: utf-8 -*-
"""SPDX-FileCopyrightText: (c) 2016 Sören Gebbert & mundialis GmbH & Co. KG.

SPDX-License-Identifier: GPL-3.0-or-later

Test AWS Sentinel service
"""

import unittest
from pprint import pprint
from flask.json import loads as json_load
from flask.json import dumps as json_dump

try:
    from .test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX
except Exception:
    from test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX


__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__ = "soerengebbert@googlemail.com"


SCENES = {
    "product_ids": [
        "S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155",
        "S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_"
        "20151207T003302",
        "S2A_MSIL1C_20170218T143751_N0204_R096_T20PRT_20170218T143931",
    ],
    "bands": ["B04", "B08"],
}

SCENES_ERROR = {
    "product_ids": [
        "S2A_MSIL1C_20170202T090201_N0204_R007_T36TVT_20170202T090155_NOPE",
        "S2A_OPER_PRD_MSIL1C_PDMC_20151207T031157_R102_V20151207T003302_"
        "20151207T003302",
        "S2A_MSIL1C_20170218T143751_N0204_R096_T20PRT_20170218T143931",
    ],
    "bands": ["B04", "B08"],
}


class AWSSentinelServiceTestCase(ActiniaResourceTestCaseBase):
    def test_1(self):
        rv = self.server.post(
            URL_PREFIX + "/sentinel2a_aws_query",
            headers=self.user_auth_header,
            data=json_dump(SCENES),
            content_type="application/json",
        )

        pprint(json_load(rv.data))

        self.assertTrue(len(json_load(rv.data)) == 3)

        self.assertEqual(
            rv.status_code,
            200,
            "HTML status code is wrong %i" % rv.status_code,
        )
        self.assertEqual(
            rv.mimetype, "application/json", "Wrong mimetype %s" % rv.mimetype
        )

    def test_error_1(self):
        rv = self.server.post(
            URL_PREFIX + "/sentinel2a_aws_query",
            headers=self.user_auth_header,
            data=json_dump(SCENES_ERROR),
            content_type="application/json",
        )

        pprint(json_load(rv.data))

        self.assertEqual(
            rv.status_code,
            400,
            "HTML status code is wrong %i" % rv.status_code,
        )
        self.assertEqual(
            rv.mimetype, "application/json", "Wrong mimetype %s" % rv.mimetype
        )


if __name__ == "__main__":
    unittest.main()

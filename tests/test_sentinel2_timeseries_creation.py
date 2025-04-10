# -*- coding: utf-8 -*-
import unittest
from pprint import pprint
from flask.json import loads as json_load
from flask.json import dumps as json_dump

try:
    from .test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX
except Exception:
    from test_resource_base import ActiniaResourceTestCaseBase, URL_PREFIX

from actinia_core.version import init_versions, G_VERSION


__license__ = "GPLv3"
__author__ = "Sören Gebbert"
__copyright__ = "Copyright 2016, Sören Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__ = "soerengebbert@googlemail.com"

# Module change example for r.slope.aspect with g.region adjustment

PRODUCT_IDS = {
    "bands": ["B04", "B08"],
    "strds": ["S2A_B04", "S2A_B08"],
    "product_ids": [
        "S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
        "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_20170227T095613",
    ],
}

# Sentinel2A time series of Germany
GERMANY = {
    "bands": ["B04", "B08"],
    "strds": ["S2A_B04", "S2A_B08"],
    "product_ids": [
        "S2A_MSIL1C_20170213T101121_N0204_R022_T32UQV_20170213T101553",
        "S2A_MSIL1C_20170213T101121_N0204_R022_T33UUR_20170213T101553",
        "S2A_MSIL1C_20170213T101121_N0204_R022_T33UUS_20170213T101553",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UMU_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UNU_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UPU_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UPV_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQB_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQC_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQD_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQE_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQU_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T32UQV_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T33UUR_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T33UUS_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T33UUT_20170216T102204",
        "S2A_MSIL1C_20170216T102101_N0204_R065_T33UUU_20170216T102204",
        "S2A_MSIL1C_20170311T103011_N0204_R108_T32UMU_20170311T103014",
        "S2A_MSIL1C_20170315T101021_N0204_R022_T32UQE_20170315T101214",
        "S2A_MSIL1C_20170410T103021_N0204_R108_T32UMU_20170410T103020",
        "S2A_MSIL1C_20170410T103021_N0204_R108_T32UNU_20170410T103020",
        "S2A_MSIL1C_20170410T103021_N0204_R108_T32UNV_20170410T103020",
        "S2A_MSIL1C_20170420T103021_N0204_R108_T32UNA_20170420T103454",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UPU_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UPV_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UQA_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UQB_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UQU_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T32UQV_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T33UUP_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T33UUQ_20170424T101120",
        "S2A_MSIL1C_20170424T101031_N0204_R022_T33UUR_20170424T101120",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UMA_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UME_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UMU_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UMV_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UND_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UNV_20170430T103024",
        "S2A_MSIL1C_20170430T103021_N0205_R108_T32UPC_20170430T103024",
        "S2A_MSIL1C_20170507T102031_N0205_R065_T32UPE_20170507T102319",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32ULB_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UMA_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UMB_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UMU_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UMV_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UNA_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UNB_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UNU_20170510T103025",
        "S2A_MSIL1C_20170510T103031_N0205_R108_T32UNV_20170510T103025",
        "S2A_MSIL1C_20170517T102031_N0205_R065_T32UQU_20170517T102352",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UMU_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UNA_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UNB_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UNC_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UNU_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UNV_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPA_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPB_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPC_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPD_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPE_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPU_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UPV_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQA_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQB_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQC_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQD_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQE_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQU_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T32UQV_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T33UUR_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T33UUS_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T33UUT_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T33UUU_20170527T102301",
        "S2A_MSIL1C_20170527T102031_N0205_R065_T33UUV_20170527T102301",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T32UPU_20170613T101608",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T32UPV_20170613T101608",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T32UQU_20170613T101608",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T32UQV_20170613T101608",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T33UUP_20170613T101608",
        "S2A_MSIL1C_20170613T101031_N0205_R022_T33UUQ_20170613T101608",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32ULB_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UMA_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UMB_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UMC_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UMU_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UMV_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UNA_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UNB_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UNC_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UNU_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UNV_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UPB_20170619T103021",
        "S2A_MSIL1C_20170619T103021_N0205_R108_T32UPC_20170619T103021",
        "S2A_MSIL1C_20170626T102021_N0205_R065_T32UMU_20170626T102321",
        "S2A_MSIL1C_20170626T102021_N0205_R065_T32UNU_20170626T102321",
        "S2A_MSIL1C_20170626T102021_N0205_R065_T32UPU_20170626T102321",
        "S2A_MSIL1C_20170626T102021_N0205_R065_T32UQU_20170626T102321",
        "S2A_MSIL1C_20170706T102021_N0205_R065_T32UNA_20170706T102301",
        "S2A_MSIL1C_20170706T102021_N0205_R065_T32UNB_20170706T102301",
    ],
}


WRONG_PRODUCT_IDS = {
    "bands": ["B04", "B08"],
    "strds": ["S2A_B04", "S2A_B08"],
    "product_ids": [
        "S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138_NOPE",
        "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_20170227T095613_NOPE",
        "S2A_MSIL1C_20170202T104241_N0204_R008_T32UNE_20170202T104236_NOPE",
    ],
}

WRONG_BANDS_IDS = {
    "bands": ["B04_NOPE", "B08_NOPE"],
    "strds": ["S2A_B04", "S2A_B08"],
    "product_ids": [
        "S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
        "S2A_MSIL1C_20170227T095021_N0204_R079_T34TBM_20170227T095613",
    ],
}

PRODUCT_IDS_ONE_WRONG = {
    "bands": ["B04", "B08"],
    "strds": ["S2A_B04", "S2A_B08"],
    "product_ids": [
        "S2A_MSIL1C_20170212T104141_N0204_R008_T31TGJ_20170212T104138",
        "S2A_MSIL1C_20170202T104241_N0204_R008_T32UNE_20170202T104236_NOPE",
    ],
}

test_mapsets = ["A"]


class AsyncSentielTimeSeriesCreationTestCaseAdmin(ActiniaResourceTestCaseBase):
    """Test the download and creation of sentinel2 time series in a new mapset
    """

    project_url_part = "projects"

    # set project_url_part to "locations" if GRASS GIS version < 8.4
    init_versions()
    grass_version_s = G_VERSION["version"]
    grass_version = [int(item) for item in grass_version_s.split(".")[:2]]
    if grass_version < [8, 4]:
        project_url_part = "locations"

    def check_remove_test_mapsets(self):
        """
        Check and remove test mapsets that have been created in the test-run
        Returns:

        """

        rv = self.server.get(
            f"{URL_PREFIX}/{self.project_url_part}/LL/mapsets",
            headers=self.admin_auth_header,
        )
        pprint(json_load(rv.data))
        self.assertEqual(
            rv.status_code,
            200,
            "HTML status code is wrong %i" % rv.status_code,
        )
        self.assertEqual(
            rv.mimetype, "application/json", "Wrong mimetype %s" % rv.mimetype
        )

        mapsets = json_load(rv.data)["process_results"]

        for mapset in test_mapsets:
            if mapset in mapsets:
                # Unlock mapset for deletion
                rv = self.server.post(
                    (f"{URL_PREFIX}/{self.project_url_part}/"
                     f"LL/mapsets/{mapset}"),
                    headers=self.admin_auth_header,
                )
                pprint(json_load(rv.data))
                # Delete the mapset if it already exists
                rv = self.server.delete(
                    (f"{URL_PREFIX}/{self.project_url_part}/"
                     f"LL/mapsets/{mapset}"),
                    headers=self.admin_auth_header,
                )
                pprint(json_load(rv.data))
                self.assertEqual(
                    rv.status_code,
                    200,
                    "HTML status code is wrong %i" % rv.status_code,
                )
                self.assertEqual(
                    rv.mimetype,
                    "application/json",
                    "Wrong mimetype %s" % rv.mimetype,
                )

    def test_1(self):
        """Test the import of two scenes with 2 bands in a new mapset A"""
        self.check_remove_test_mapsets()

        rv = self.server.post(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/A/sentinel2_import"),
            headers=self.admin_auth_header,
            data=json_dump(PRODUCT_IDS),
            content_type="application/json",
        )

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(rv, headers=self.admin_auth_header)

        rv = self.server.get(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/A/strds"),
            headers=self.admin_auth_header,
        )
        pprint(json_load(rv.data))
        self.assertEqual(
            rv.status_code,
            200,
            "HTML status code is wrong %i" % rv.status_code,
        )
        self.assertEqual(
            rv.mimetype, "application/json", "Wrong mimetype %s" % rv.mimetype
        )

        strds_list = json_load(rv.data)["process_results"]
        self.assertTrue("S2A_B04" in strds_list)
        self.assertTrue("S2A_B08" in strds_list)

    def test_1_error_mapset_exists(self):
        """PERMANENT mapset exists. hence an error message is expected"""
        rv = self.server.post(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/PERMANENT/sentinel2_import"),
            headers=self.admin_auth_header,
            data=json_dump(PRODUCT_IDS),
            content_type="application/json",
        )

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(
            rv,
            headers=self.admin_auth_header,
            http_status=400,
            status="error",
            message_check="AsyncProcessError:",
        )

    def test_2_error_wrong_product_ids(self):
        """Check for all wrong product ids

        Returns:

        """
        self.check_remove_test_mapsets()

        rv = self.server.post(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/A/sentinel2_import"),
            headers=self.admin_auth_header,
            data=json_dump(WRONG_PRODUCT_IDS),
            content_type="application/json",
        )

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(
            rv,
            headers=self.admin_auth_header,
            http_status=400,
            status="error",
            message_check="AsyncProcessError:",
        )

    def test_3_error_wrong_bands_ids(self):
        """Check for wrong band ids"""
        self.check_remove_test_mapsets()

        rv = self.server.post(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/A/sentinel2_import"),
            headers=self.admin_auth_header,
            data=json_dump(WRONG_BANDS_IDS),
            content_type="application/json",
        )

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(
            rv,
            headers=self.admin_auth_header,
            http_status=400,
            status="error",
            message_check="AsyncProcessError:",
        )

    def test_4_error_wrong_product_id(self):
        """Check if a missing product id is found"""
        self.check_remove_test_mapsets()

        rv = self.server.post(
            (f"{URL_PREFIX}/{self.project_url_part}/"
             "LL/mapsets/A/sentinel2_import"),
            headers=self.admin_auth_header,
            data=json_dump(PRODUCT_IDS_ONE_WRONG),
            content_type="application/json",
        )

        pprint(json_load(rv.data))
        self.waitAsyncStatusAssertHTTP(
            rv,
            headers=self.admin_auth_header,
            http_status=400,
            status="error",
            message_check="AsyncProcessError:",
        )


if __name__ == "__main__":
    unittest.main()

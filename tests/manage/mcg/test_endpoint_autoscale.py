import logging
import time
import pytest

from ocs_ci.framework.testlib import tier1
from ocs_ci.ocs.resources.pod import get_pods_having_label, get_pod_count
from ocs_ci.ocs import constants, defaults


@pytest.mark.polarion_id("OCS-XXXX")
@tier1
class TestBucketCreation(MCGTest):
    """
    Test MCG endpoint auto-scaling

    """

    # This will ensure the test will start
    # with an autoscaling conifguration of 1-2
    MIN_ENDPOINT_COUNT = 1
    MAX_ENDPOINT_COUNT = 2

    def test_scaling_ender_load(self):
        self._start_load_job()
        self._assert_endpoint_count(2)

        self._end_load_job()
        self._assert_endpoint_count(1)

    def _start_load_job():
        pass

    def _end_load_job():
        pass

    def _assert_endpoint_count(self, desired_count)
        pod = ocp.OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)

        assert self.pod.wait_for_resource(
            resource_count=desired_count,
            condition=constants.STATUS_RUNNING,
            selector=constants.NOOBAA_ENDPOINT_POD_LABEL,
            dont_allow_other_resources=True,
            timeout=300,
        )

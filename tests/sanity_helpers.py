import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.utility.utils import ceph_health_check, run_cmd, TimeoutSampler
from ocs_ci.utility import templating
from ocs_ci.ocs.cluster import CephCluster, CephClusterExternal


logger = logging.getLogger(__name__)


class Sanity:
    """
    Class for cluster health and functional validations
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.ceph_cluster = CephCluster()

    def health_check(self, cluster_check=True, tries=20):
        """
        Perform Ceph and cluster health checks
        """
        wait_for_cluster_connectivity(tries=400)
        logger.info("Checking cluster and Ceph health")
        node.wait_for_nodes_status(timeout=300)

        ceph_health_check(namespace=config.ENV_DATA['cluster_namespace'], tries=tries)
        if cluster_check:
            self.ceph_cluster.cluster_health_check(timeout=60)

    def create_resources(self, pvc_factory, pod_factory, run_io=True):
        """
        Sanity validation - Create resources (FS and RBD) and run IO

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            run_io (bool): True for run IO, False otherwise

        """
        logger.info("Creating resources and running IO as a sanity functional validation")

        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(interface)
            self.pvc_objs.append(pvc_obj)
            self.pod_objs.append(pod_factory(pvc=pvc_obj, interface=interface))
        if run_io:
            for pod in self.pod_objs:
                pod.run_io('fs', '1G', runtime=30)
            for pod in self.pod_objs:
                get_fio_rw_iops(pod)

    def delete_resources(self):
        """
        Sanity validation - Delete resources (FS and RBD)

        """
        logger.info("Deleting resources as a sanity functional validation")

        for pod_obj in self.pod_objs:
            pod_obj.delete()
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)


class SanityExternalCluster(Sanity):
    """
    Helpers for health check and functional validation
    in External mode
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.ceph_cluster = CephClusterExternal()
        self.create_obc()
        self.verify_obc()

    def create_obc(self):
        """
        OBC creation for RGW and Nooba
        only applicable for external cluster

        """
        obc_rgw = templating.load_yaml(
            constants.RGW_OBC_YAML
        )
        obc_rgw_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='obc_rgw_data', delete=False
        )
        templating.dump_data_to_temp_yaml(
            obc_rgw, obc_rgw_data_yaml.name
        )
        logger.info("Creating OBC for rgw")
        run_cmd(f"oc create -f {obc_rgw_data_yaml.name}", timeout=2400)

        obc_nooba = templating.load_yaml(
            constants.MCG_OBC_YAML
        )
        obc_mcg_data_yaml = tempfile.NamedTemporaryFile(
            mode='w+', prefix='obc_mcg_data', delete=False
        )
        templating.dump_data_to_temp_yaml(
            obc_nooba, obc_mcg_data_yaml.name
        )
        logger.info("create OBC for mcg")
        run_cmd(f"oc create -f {obc_mcg_data_yaml.name}", timeout=2400)

    def verify_obc(self):
        """
        OBC verification from external cluster perspective,
        we will check 2 OBCs

        """
        sample = TimeoutSampler(
            300,
            5,
            self.ceph_cluster.noobaa_health_check
        )
        sample.wait_for_func_status(True)

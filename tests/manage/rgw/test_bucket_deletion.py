import logging

import botocore
import pytest
from tests import helpers

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    acceptance, tier1, tier3
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectbucket import OBC

logger = logging.getLogger(__name__)


class TestBucketDeletion:
    """
    Test bucket Creation Deletion of buckets
    """
    @pytest.mark.parametrize(
        argnames="amount,interface",
        argvalues=[
            pytest.param(
                *[3, 'RGW-OC'],
                marks=[pytest.mark.polarion_id("OCS-1939"), tier1, acceptance]
            ),
        ]
    )
    def test_bucket_delete(self, rgw_bucket_factory, amount, interface):
        """
        Test deletion of bucket using the S3 SDK, MCG CLI and OC
        """
        for bucket in rgw_bucket_factory(amount, interface):
            logger.info(f"Deleting bucket: {bucket.name}")
            assert bucket.delete()

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *['RGW-OC'],
                marks=[tier1, pytest.mark.polarion_id("OCS-1868")]
            ),
        ]
    )
    def test_bucket_delete_with_objects(self, rgw_bucket_factory, interface, awscli_pod):
        """
        Negative test with deletion of bucket has objects stored in.
        """
        bucket = rgw_bucket_factory(1, interface)[0]
        bucketname = bucket.name
        obc_obj = OBC(bucketname)
        try:
            data_dir = '/data'
            full_object_path = f"s3://{bucketname}"
            helpers.retrieve_test_objects_to_pod(awscli_pod, data_dir)
            helpers.sync_object_directory(
                awscli_pod, data_dir, full_object_path, obc_obj
            )

            logger.info(f"Deleting bucket: {bucketname}")
            if interface == "S3":
                try:
                    s3_del = obc_obj.s3_resource.Bucket(bucketname).delete()
                    assert not s3_del, (
                        "Unexpected s3 delete non-empty OBC succeed"
                    )
                except botocore.exceptions.ClientError as err:
                    assert "BucketNotEmpty" in str(err), (
                        "Couldn't verify delete non-empty OBC with s3"
                    )
                    logger.info(
                        f"Delete non-empty OBC {bucketname} failed as expected"
                    )
        finally:
            bucket.delete()

    @pytest.mark.parametrize(
        argnames="interface",
        argvalues=[
            pytest.param(
                *['RGW-OC'],
                marks=[tier3, pytest.mark.polarion_id("OCS-1400")]
            ),
        ]
    )
    def test_nonexist_bucket_delete(self, interface):
        """
        Negative test with deletion of non-exist OBC.
        """
        name = "test_nonexist_bucket_name"
        if interface == "RGW-OC":
            try:
                oc_del = OCP(
                    kind='obc', namespace=config.ENV_DATA['cluster_namespace']
                ).delete(resource_name=name)
                assert oc_del, "Unexpected oc delete non-exist OBC succeed"
            except CommandFailed as err:
                assert "NotFound" in str(err), (
                    "Couldn't verify delete non-exist OBC with oc"
                )
        logger.info(
            f"Delete non-exist OBC {name} failed as expected"
        )
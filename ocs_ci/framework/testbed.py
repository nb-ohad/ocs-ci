import logging
import requests

from getpass import getuser


HOSTPORT = 'http://ocsdb.ceph.redhat.com:3000'
log = logging.getLogger(__name__)


class InvalidRequest(Exception):
    pass


class RequestNotMet(Exception):
    pass


class TestbedPostFailed(Exception):
    pass


class TestbedReleaseFailed(Exception):
    pass


def reserve_testbed(launch_name, testbed_name, timeout):
    """
    Function to reserve the testbed

    Args:
        launch_name(str): launch name of the run
        testbed_name(str): testbed_name to reserve
        timeout(int): timeout after timeout seconds if unable to reserve

    """
    user = getuser()
    r_testbed_ep = HOSTPORT + '/testbedrequest'
    data = dict()
    if testbed_name is None:
        data = {
            'testrunname': launch_name,
            'username': user,
            'timeout': timeout,
        }
    else:
        data = {
            'testrunname': launch_name,
            'testbedname': testbed_name,
            'username': user,
        }
    resp = requests.post(r_testbed_ep, json=data)
    log.info(resp.status_code)
    if resp.status_code == 201:
        log.info("Post is successful")
    else:
        log.error(f"post failed {resp.json()}")
        raise TestbedPostFailed


def check_request_met(launch_name):
    """
    Function to check if the reservation is met

    Args:
        launch_name(str): launch name of the run

    Returns:
        requestmet(bool): True if reservation is successful
        testbedname(str): Name of the testbed reserved
        config(str): Optional configuration of the testbed

    """
    r_testbed_ep = HOSTPORT + '/testbedrequest'
    params = {
        'testrunname': f'eq.{launch_name}',
    }
    resp = requests.get(r_testbed_ep, params=params)
    if len(resp.json()) == 0:
        log.error("Invalid request")
        raise InvalidRequest
    if resp.status_code == 200:
        log.info("Successfuly sent the request")
        return (
            resp.json()[0]['requestmet'],
            resp.json()[0]['testbedname'],
            resp.json()[0]['config']
        )
    else:
        raise RequestNotMet


def release_testbed(testbed_name):
    """
    Function to release the testbed

    Args:
        testbed_name(str): name of the testbed to release

    """
    a_testbed_ep = HOSTPORT + '/testbed'
    if testbed_name is None:
        log.error("Testbed Name is required")
        exit(1)
    data = dict()
    params = {'testbedname': f'eq.{testbed_name}'}
    data = {
        'reserved': False
    }
    resp = requests.patch(a_testbed_ep, params=params, json=data)
    log.info(resp.status_code)
    if resp.status_code == 404:
        log.error("Invalid Testbed Name")
        raise TestbedReleaseFailed
    if resp.status_code == 400:
        log.error(f"Failed to release the testbed {testbed_name}")
        raise TestbedReleaseFailed
    else:
        log.info(f"Successfully released the testbed {testbed_name}")

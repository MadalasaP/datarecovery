# !/usr/bin/env python
# coding: utf-8
#
# Copyright 2020 Tsecond Inc. All Rights Reserved.
#
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential
#
# Written by: Manavalan Krishnan 11/12/20
#
from logging import debug
from libutils import run_cmd
from json import loads
import concurrent.futures
from itertools import repeat
import multiprocessing as mp


def parallel_exe(func_name,*values, num_threads = 0.8*mp.cpu_count()):
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_threads) as executor:
      results = executor.map(func_name, *values)
    return results


def nvme_list_drives():
    """ Enumerates all NVME drives in the system
    Returns:
        return code and value tuple
        return code: 0 for success and non-zero for failure
        value: list of drives as json on success or error string on failure
    """
    debug("Listing all nvme drives in the system")
    cmd = "sudo nvme list -o json"
    rc, stdout, stderr = run_cmd(cmd)
    out = stderr
    if not rc:
        out = loads(stdout)['Devices']
    return rc, out


def sata_list_drives():
    """ Enumerates all SATA drives in the system
        Returns:
            return code and value tuple
            return code: 0 for success and non-zero for failure
            value: list of drives as json on success or error string on failure
        """
    debug("Listing all sata drives in the system")
    cmd = "lsblk -o KNAME,TYPE,SIZE,SERIAL,MODEL,SUBSYSTEMS --json -b"
    rc, stdout, stderr = run_cmd(cmd)
    out = stderr
    if not rc:
        out = loads(stdout)['blockdevices']
    return rc, out


def nvme_get_drive_info(drive):
    """ Gets the detailed runtime info about the drive
    Args:
        drive: drive path ex. /dev/nvme0n1
    Returns:
        return code and value tuple
        return code: 0 for success and non zero for failure
        value: json document on success or error string
               on failure
    """
    debug("Getting detailed information for the drive {}".format(drive))
    cmd = 'sudo nvme smart-log {} -o json'.format(drive)
    rc, stdout, stderr = run_cmd(cmd)
    out = stderr
    if not rc:
        out = stdout
    return rc, out


def nvme_erase_drive(drive):
    """ Securely erases the drive content
    Args:
        drive: drive path ex. /dev/nvme0n1
    Returns:
        return code and value tuple
        return code: 0 for success and non zero for failure
        value: error string on failure, success message on success
    """
    debug("Securely erasing the drive {}".format(drive))
    cmd = "sudo nvme format --force --ses=1 {}".format(drive)
    return run_cmd(cmd)


def sata_erase_drive(drive):
    """ Securely erases the drive content
    Args:
        drive: drive path ex. /dev/nvme0n1
    Returns:
        return code and value tuple
        return code: 0 for success and non zero for failure
        value: error string on failure, success message on success
    """

    return 0, 'Erase Success of drive {}'.format(drive), ''


def nvme_erase_drives(drives, parallel=False):
    """ Securely erases the drive content of given drives
    Args:
        drives: list of drive paths ex. /dev/nvme0n1,/dev/nvme1n1
        parallel: Execute the erasing in parallel
    Returns:
        return code, stdout and stderr tuple
        return code: 0 for success and non zero for failure
    """
    debug("Securely erasing drives: {}".format(",".join(drives)))
    errmsg = ""
    outmsg = ""
    rc = 0
    results = []
    if(parallel):
        # values = drives,repeat(keyfile)
        results = parallel_exe(nvme_erase_drive,drives)
    else:
        for drive in drives:
            results.append(nvme_erase_drive(drive))

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
    return return_code, outmsg, errmsg

    # if not parallel:
    #     for drive in drives:
    #         rcode, out, err = nvme_erase_drive(drive)
    #         if rcode:
    #             rc = 1
    #             err_msg += err
    #         out_msg += out
    # return rc, out_msg, err_msg


def sata_erase_drives(drives, parallel=False):
    """ Securely erases the drive content of given drives
    Args:
        drives: list of drive paths ex. /dev/nvme0n1,/dev/nvme1n1
        parallel: Execute the erasing in parallel
    Returns:
        return code, stdout and stderr tuple
        return code: 0 for success and non zero for failure
    """
    debug("Securely erasing drives: {}".format(",".join(drives)))
    errmsg = ""
    outmsg = ""
    rc = 0
    results = []
    if(parallel):
        # values = drives,repeat(keyfile)
        results = parallel_exe(sata_erase_drive,drives)
    else:
        for drive in drives:
            results.append(sata_erase_drive(drive))

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            rc = 1
            outmsg += out
    return rc, outmsg, errmsg

    # if not parallel:
    #     for drive in drives:
    #         rcode, out, err = nvme_erase_drive(drive)
    #         if rcode:
    #             rc = 1
    #             err_msg += err
    #         out_msg += out
    # return rc, out_msg, err_msg
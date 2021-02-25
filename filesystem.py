from logging import debug
from libutils import *
import time
import os

def filesystem_create(drive):
    """Creates a filesystem on drive.
    Args:
    drive: Name of the drive that contains the partition to be delete
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("creating file system")
    time_to_wait = 500
    while not os.path.exists(drive):
         time.sleep(0.032)
         time_to_wait = time_to_wait - 1
         if(time_to_wait<0):
             return 1,"","Path doesn't exist"
    cmd = "sudo mkfs.xfs -f -K " + drive
    return run_cmd(cmd)


def filesystem_mount(drive, path, create=False):
    """mounts a drive to the given path.
    Args:
    drive: Name of the drive that needs to be mounted
    path: path for the where the drive to be mounted
    path:
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("Mounting the drive : {} to path : {}".format(drive, path))
    if create:
         cmd = "mkdir -p " + path
         rc, out, err = run_cmd(cmd)
         if rc:
             return rc, out, err
    cmd = "sudo mount " + drive + " " + path
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err
    cmd = "sudo chmod 777 "+path
    return run_cmd(cmd)


def filesystem_unmount(path):
    """mounts a drive to the given path.
    Args:
    drive: Name of the drive that needs to be mounted
    path: path for the where the drive to be mounted
    path:
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("Unmounting the drive at path : {}".format(path))
    cmd = "sudo umount " + path
    return run_cmd(cmd)


def filesystem_flush():
    debug("Flushing file system cache")
    cmd = "sync"
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err
    cmd = "sudo sysctl -w vm.drop_caches=3"
    return run_cmd(cmd)


def filesystem_is_mounted(drive):
    """ checks if given drive is mounted. This function is used by Bryck
    functions to check if Bryck is already mounted
    Args:
        drive: Name of the drive to be checked
    Returns:
        True if the drive is mounted or False if not
    """
    debug("Check if the drive {} is mounted".format(drive))
    with open("/proc/mounts") as proc_file:
        for line in proc_file:
            if drive == line.split()[0]:
                return True
        return False


def filesystem_usage(mount_dir):
    fsdata = {}
    cmd = "df {} | grep -v Filesystem".format(mount_dir)
    rc, out, err = run_cmd(cmd)
    if rc:
        return 1, fsdata
    fields = out.split()
    fsdata['usable_capacity'] = round(int(fields[1])/(1024 * 1024),2)
    fsdata['used_space'] = round(int(fields[2])/(1024 * 1024),2)
    fsdata['available_space'] = round(int(fields[3])/(1024 * 1024),2)
    fsdata['usage'] = fields[4]
    return 0, fsdata

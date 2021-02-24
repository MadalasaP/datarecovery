from logging import debug
from libutils import run_cmd


def data_protection_setup(devices, raid_name, raid_level):
    """creates a raid with given set of drives.
    Args:
    devices: list of the devices that needs to be included in the raid
    raid_name: The name of the logical unit of raid that will be created
    raid_level: level of raid that needs to be implemented.
    Returns:
    A tuple: return code, stderr, stdout"""
    data_protection_reset(raid_name, devices)
    debug("Creating raid {} level {} devices: {} ".format(raid_name, raid_level,
                                                          ','.join(devices)))
    cmd = "yes | sudo mdadm --create  " + raid_name + " --level=" + str(raid_level) + \
          " --raid-devices=" + str(len(devices)) + " " + ' '.join(devices) + ""
    return run_cmd(cmd)


def data_protection_stop(raid_name):
    """stops a raid.
    Args:
    raid_name: The name of the raid to be stopped
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("Stopping raid {}".format(raid_name))

    raid_dev = data_protection_get_dev(raid_name)
    if not raid_dev:
        debug("Raid "+raid_name+" not running")
        return 0, "", ""
    cmd = "sudo mdadm --stop  " + raid_name + ""
    return run_cmd(cmd)


def data_protection_start():
    """scans for raid devices and activates them
    Args:
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("Starting raid")
    cmd = "sudo mdadm --assemble --scan"
    rc, err, out = run_cmd(cmd)

    if rc:
        cmd = "sudo mdadm --examine --scan | sudo tee -a /etc/mdadm/mdadm.conf"
        return run_cmd(cmd)

    return rc,err,out


def data_protection_reset(raid_device, devices):
    """resets all the devices in raid.
    Args:
    devices: list of devices that needs to be reset.
    partition_number: Number of the partition to be delete
    Returns:
    A tuple: return code, stderr, stdout"""
    debug("Resetting raid on devices {}".format(','.join(devices)))

    return_code = 0
    out_msg = ""
    err_msg = ""
    cmd="sudo mdadm --stop "+raid_device
    run_cmd(cmd)
    cmd = "sudo mdadm --zero-superblock " + ' '.join(devices) + ""
    return run_cmd(cmd)


def data_protection_get_dev(raid_name):
    cmd = "ls -l " +raid_name
    rc, out, err = run_cmd(cmd)
    if rc:
        return ""
    dev = "/dev/"+out.split("/")[-1]
    return dev.rstrip("\n")

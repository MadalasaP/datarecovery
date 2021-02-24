from logging import debug
from libutils import run_cmd


def partition_create_label(drive, label):
    """Creates a partition label for a drive.
    Args:
    drive: Name of the drive to be labeled
    label: the label that need to be assigned
    Returns:
    A tuple: return code, stderr, stdout
    """
    debug("creating partition label for drive {}".format(drive))
    cmd = "sudo parted -s " + drive + " mklabel " + label + ""
    return run_cmd(cmd)


def partition_create_drive(drive, size):
    """Creates a partition of give size
    Args:
    drive: Name of the drive where partition to be created
    size: size of the partition in GB. -1 for remaining free space
    Returns:
    A tuple: return code, stderr, stdout
    """

    debug("Creating partition on drive {} of size {} GB".format(drive, size))
    cmd = "sudo parted -s " + drive + " unit MB print free | grep Free | tail -n 1"
    returncode, stdout, stderr = run_cmd(cmd)
    fields_mb = stdout.split()
    if returncode == 0 and len(fields_mb) >= 2:
        start = float(fields_mb[0].replace("MB",""))
        end = float(fields_mb[1].replace("MB",""))
        start = 1 if start < 1 else start
        end = end if size < 0 else start + size
        cmd = "sudo parted -s " + drive + " mkpart primary " + str(start) + \
              " " + str(end) + ""
        return run_cmd(cmd)
    else:
        return returncode, stdout, stderr


def partition_delete_drive(drive, partition_number):
    """ Deletes a partition on a given drive
    Args:
    drive: Name of the drive that contains the partition to be deleted
    partition_number: Number of the partition to be delete
    Returns:
    A tuple: return code, stderr, stdout
    """
    debug("Deleting partition {} on drive {}".format(partition_number, drive))
    cmd = "sudo parted -s " + drive + " rm " + partition_number
    return run_cmd(cmd)

def partition_create_drives(drives, size, mklabel=False):
    """ create parition on given list of drives
    Args:
    drives: List of drive names
    size: size of the parition
    mklabel: check if label needs to be created:w
    Returns:
    A tuple: return code, stderr, stdout
    """
    err_msg = ""
    out_msg = ""
    return_code = 0
    for drive in drives:
        if mklabel:
            rc, out, err = partition_create_label(drive,"gpt")
            if rc:
                return rc, out, err
        rc, out, err = partition_create_drive(drive, size)
        if rc:
            err_msg += err
            return_code = 1
        out_msg += out
    return return_code, out_msg, err_msg

def partition_reset_drives(drives):
    """ Remove all partitions from the drive
    Args:
        drive: Device that needs partitions deleted
    Returns:
        A tuple: return code, stderr, stdout 
    """    		
    err_msg = ""
    out_msg = ""
    return_code = 0
    for drive in drives:
        cmd="sudo parted -s "+drive+" print 2>/dev/null |awk '/^ / {print $1}'"
        rc, out, err = run_cmd(cmd)
        if rc:
            #No partition. return success
            return 0, out, err
        #delete all partitions
        partitions = out.strip().split("\n")
        for part in partitions:
            rc, out, err = partition_delete_drive(drive, part)
            if rc:
                return_code = 1
            err_msg += err
            out_msg += out
    return return_code, out_msg, err_msg
 


from logging import debug
from libutils import run_cmd
from os import path,cpu_count
import multiprocessing as mp
import concurrent.futures
from itertools import repeat

def parallel_exe(func_name,*values, num_threads = int(0.8*mp.cpu_count())):
    with concurrent.futures.ProcessPoolExecutor(max_workers = num_threads) as executor:
      results = executor.map(func_name, *values)
    return results

def encrypt_is_enabled(drives):
    for drive in drives:
        cmd = "sudo blkid " + drive
        rc, out, err = run_cmd(cmd)
        if rc:
            continue
        if "crypto_LUKS" in out:
            return True
    return False

def encrypt_setup_drive(drive, key_path):
    """setups encryption on a drive.
    Args:
    drive: Name of the drive that that needs to encrypted
    key_path: path of the key file
    Returns:
    A tuple: return code, stderr, stdout
    """
    debug("Setting up the encryption for Drive : " + drive)
    cmd = "sudo cryptsetup -q luksFormat " + drive + " " + key_path + ""
    return run_cmd(cmd)


def encrypt_unlock_drive(drive, key_path):
    """Unlocking an encrypted drive.
    Args:
    drive: Name of the drive that that needs to be unlocked
    key_path: path of the key file
    Returns:
    A tuple: return code, stderr, stdout
    """
    encrypt_drive = encrypt_drive_name(drive)
    debug("Unlocking the drive " + encrypt_drive)
    cmd = "sudo cryptsetup open --key-file " + key_path + " " + drive + \
          "  " + encrypt_drive + ""
    rc, out, err = run_cmd(cmd)
    if rc:
        return rc, out, err, drive
    cmd = "sudo partprobe"
    run_cmd(cmd)
    return 0, "", "", None


def encrypt_lock_drive(drive):
    """Lock an encrypted drive
    Args:
    drive: Name of the drive to be locked
    Returns:
    A tuple: return code, stderr, stdout
    """
    encrypt_drive = encrypt_drive_name(drive)
    # deactivate any partitions in the drive
    debug("Deactivating partitions on " + encrypt_drive)

    cmd = "ls /dev/mapper/{}[p]*".format(encrypt_drive) #NVME partition p1 and p2
    rc, out, err = run_cmd(cmd)
    if rc:
        cmd = "ls /dev/mapper/{}[1-2]".format(encrypt_drive) #SATA partition 1 and 2
        rc, out, err = run_cmd(cmd)

    if out:
       parts = out.strip().rstrip("\n").split()
       for part in parts:
           cmd = "sudo dmsetup remove "+part
           rc, out, err = run_cmd(cmd)
           if rc:
               return rc, out, err
    debug("Locking the encrypted drive " + encrypt_drive)
    cmd = "sudo cryptsetup close " + encrypt_drive + ""
    return run_cmd(cmd)


def encrypt_reset_drive(drive):
    """resets encryption on a drive.
    Args:
    drive: Name of the drive to be reset
    Returns:
    A tuple: return code, stderr, stdout
    """
    debug("Resetting the encryption on drive " + drive)
    encrypt_lock_drive(drive)
    encrypt_drive = encrypt_drive_name(drive)
    cmd = "sudo cryptsetup remove " + encrypt_drive + ""
    return run_cmd(cmd)


def encrypt_drive_name(drive):
    """Returns a name for the encrypted drive
    Args:
    drive: Name of the drive
    Returns:
    Logical name that represents the encrypted device
    """
    return 'crypt' + drive.split('/')[-1]


def encrypt_reset_drives(drives, parallel=True):
    debug("Setting encryption on drives:" + ','.join(drives))
    errmsg = ""
    outmsg = ""
    return_code = 0

    if(parallel):
        results = parallel_exe(encrypt_reset_drive,drives)
    else:
        for drive in drives:
            results.append(encrypt_reset_drive(drive))

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
    return return_code, outmsg, errmsg


def encrypt_setup_drives(drives, keyfile, parallel=True):
    debug("Setting up encryption on drives:" + ','.join(drives))
    errmsg = ""
    outmsg = ""
    return_code = 0

    if(parallel):
        values = drives,repeat(keyfile)
        results = parallel_exe(encrypt_setup_drive,*values)
    else:
        for drive in drives:
            results.append(encrypt_setup_drive(drive))

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
    return return_code, outmsg, errmsg


def encrypt_lock_drives(drives, parallel=True):
    debug("Locking the drives:" + ','.join(drives))

    errmsg = ""
    outmsg = ""
    return_code = 0

    if(parallel):
        results = parallel_exe(encrypt_lock_drive,drives)
    else:
        for drive in drives:
            results.append(encrypt_lock_drive(drive))

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
    return return_code, outmsg, errmsg


def encrypt_change_key_drive(drive, old_key, new_key):
    debug("Changing the keys for the drive:" + drive)
    cmd = "sudo cryptsetup luksChangeKey {} --key-file {} {}".format(drive,
                                           old_key, new_key)
    return run_cmd(cmd)


def encrypt_change_key_drives(drives, old_key, new_key):
    debug("Changing key for the drives:" + ','.join(drives))
    errmsg = ""
    outmsg = ""
    return_code = 0

    values = drives,repeat(old_key),repeat(new_key)
    results = parallel_exe(encrypt_change_key_drive,*values)

    for result in results:
        rc, out, err = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
    return return_code, outmsg, errmsg

    # for drive in drives:
    #     rc, out, err = encrypt_change_key_drive(drive, old_key, new_key)
    #     if rc:
    #         errmsg += err
    #         return_code = 1
    #     outmsg += out
    # return return_code, outmsg, errmsg


def encrypt_unlock_drives(drives, keyfile, parallel=True):
    # debug(" Unlocking the drive " + drives)
    debug("Unlocking the drives:" + ','.join(drives))
    errmsg = ""
    outmsg = ""
    return_code = 0
    results = []
    errdrive = []

    if parallel:
        values = drives,repeat(keyfile)
        results = parallel_exe(encrypt_unlock_drive,*values)
    else:
        for drive in drives:
            results.append(encrypt_unlock_drive(drive,keyfile))

    for result in results:
        rc, out, err, drive = result
        if rc:
            errmsg += err
            return_code = 1
            outmsg += out
            if not drive is None:
                errdrive.append(drive)

    return return_code, outmsg, errmsg, errdrive

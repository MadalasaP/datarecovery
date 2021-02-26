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
from logging import debug, info, DEBUG, INFO, basicConfig
from json import load, dumps, dump
from os.path import dirname, exists
from nvme import nvme_list_drives, nvme_erase_drives, sata_list_drives, sata_erase_drives
from data_protection import *
from partition import *
from encryption import *
from filesystem import *
from datetime import datetime
from time import tzname
import subprocess
import json

def get_config():
    """ Get the Bryck static configuration parameters"""
    config = dirname(__file__) + "/config.json"
    with open(config) as cfg:
        json_cfg = load(cfg)
        return json_cfg

def get_version():
    """ Reads the version from the config json and returns it"""
    return get_config()['version']

def get_tsutil_name():
    """ Reads the version from the config json and returns it"""
    return get_config()['tsutil_name']

def set_log_level(verbose):
    log_level = INFO
    if verbose:
        log_level = DEBUG
    basicConfig(format='%(message)s', level=log_level)


class Bryck(object):

    def __init__(self, verbose=0):
        self.config = get_config()
        self.messages = self.load_messages()
        self.drives = self.get_drives()
        self.verbose = verbose
        self.encryption = False
        self.data_protection = 'raid'
        self.data_raid_level = 5
        self.metadata_raid_level = 1
        self.format_time = None
        self.serial_number = None
        self.firmware_rev = 1.0
        self.raw_capacity = 0
        self.product_name = self.messages['product_name']
        if self.drives:
            if self.config['drive_type'] == 'NVME':
                self.firmware_rev = self.drives[0]['Firmware']
            else:
                self.firmware_rev = 'Firmware'
            self.serial_number = self.drives[0]['SerialNumber']
            capacity = 0
            for drive in self.drives:
                capacity += int(drive['PhysicalSize'])
            self.raw_capacity = capacity/(1024 * 1024 * 1024)


    def load_messages(self):
        """ Load the error and information messages"""
        messages = dirname(__file__) + "/messages.json"
        with open(messages) as msg:
            json_msg = load(msg)
            return json_msg

    def is_mounted(self):
        """ Returns True if the Bryck is already mounted"""
        raid_dev = data_protection_get_dev(self.config['data_drive_name'])
        meta_dev = data_protection_get_dev(self.config['metadata_drive_name'])
        if not raid_dev and not meta_dev:
            return False
        data_fs = filesystem_is_mounted(raid_dev)
        metadata_fs = filesystem_is_mounted(meta_dev)
        return ( data_fs or metadata_fs )

    def get_drive_names(self):
        """ Gets list of drive names from the drives list"""
        drive_names = []
        for drive in self.drives:
            drive_names.append(drive['DevicePath'])
        return drive_names

    def get_encrypt_drive_names(self, drives):
        """ Returns the encrypted names for each drive
        Args:
            drives: List of drive names
        Returns:
            Names of the encrypted device
        """
        enc_names = []
        for drive in drives:
            enc_names.append("/dev/mapper/"+encrypt_drive_name(drive))
        return enc_names

    def create_partitions(self, drives):
        """ Creates metadata and data partitions on the given drives
        Args:
            drives: List of drives where the partitions to be created
        Returns:
            Return code and message tuple
            Return code: 0 for success and nob zero for failure
            message: Description for the error code
        """

        # Create metadata partition
        rc, out, err = partition_create_drives(drives,
                                            self.config['metadata_part_size'],
                                            mklabel=True)
        if rc:
            return rc, err
        # Create data partition on the remaining space
        rc, out, err = partition_create_drives(drives, -1)
        if rc:
            out = err
        return rc, out

    def get_partition_names(self, ptype, drive_names):
        """ Returns the partition names for a given drive name
        Args:
            ptype: Type of the parition (meta or data)
            drive_names: Names of the drives that need parition names
        Returns:
            Returns the partition names corresponding to the given drive names
        """
        part_names = []
        for drive in drive_names:
            if self.config['drive_type'] == 'NVME':
                part = "p1"
            else:
                part = "1"
            if ptype == "data":
                if self.config['drive_type'] == 'NVME':
                    part = "p2"
                else:
                    part = "2"
            part_names.append(drive + part)
        return part_names

    def write_metadata(self, mount_meta=True):
        """ Writes metadata to the Bryck metadata partition
        Args:
            mount_meta: decides if this function should mount the Bryck
            before writing metadata
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """

        # form the metadata
        meta = {}
        meta["encryption"] = self.encryption
        meta["data_protection"] = self.data_protection
        meta["data_raid_level"] = self.data_raid_level
        meta["metadata_raid_level"] = self.metadata_raid_level
        meta["format_time"] = self.format_time
        meta["serial_number"] = self.serial_number
        meta["drives"] = self.drives
        #json_meta = dumps(meta)

        #logical card store.json
        if os.path.exists(self.config['agylagent_lc_store']):
            with open(self.config['agylagent_lc_store'], 'r+') as lc_file:
               json_meta = load(lc_file)
               info(json_meta)
            meta.update(json_meta)
        else:
            meta = dumps(meta)

        if mount_meta:
            rc, out, err = filesystem_mount(self.config['metadata_drive_name'],
                                            self.config['metadata_mount'],
                                            create=True)
            if rc:
                return 1, self.messages['metadata_write_fail'] + err

        metafile = self.config['metadata_mount'] + "/" + \
                   self.config['metadata_file_name']
        fptr = open(metafile, "w")
        if not fptr:
            return 1, self.messages['metadata_write_fail'] + \
                   "Failed to open metadata file {}".format(metafile)
        dump(meta, fptr)
        fptr.close()

        #BSMB store.json file
        if os.path.exists(self.config['agylagent_bsmb_store']):
            with open(self.config['agylagent_bsmb_store'], 'r+') as bsmb_file:
                data = load(bsmb_file)

            metafile = self.config['metadata_mount'] + "/" + self.config['metadata_bsmb_file_name']
            fptr = open(metafile, "w")
            if not fptr:
                return 1, self.messages['metadata_write_fail'] + \
                       "Failed to open metadata file {}".format(metafile)
            dump(data, fptr)
            fptr.close()

        if not os.path.exists(self.config['metadata_mount'] + self.config['backup_dir']):
            os.mkdir(self.config['metadata_mount'] + self.config['backup_dir'])

        rc, msg = encrypt_backup(self.config['metadata_mount'] + self.config['backup_dir']
                                 ,self.get_drive_names())
        if rc:
            debug(msg)

        drive_names = self.get_drive_names()
        rc, msg = partition_backup(self.config['metadata_mount'] + self.config['backup_dir'],
                                   self.get_encrypt_drive_names(drive_names))
        if rc:
            debug(msg)

        if mount_meta:
            filesystem_unmount(self.config['metadata_mount'])
        return 0, ""

    def format(self, no_auth=False, no_enc=False, no_erase=False,
               raid_chunk=1, raid_level=5, key_file=None):
        """ Formats the Bryck and sets up a mountable encrypted file system
        Args:
            no_auth: disable authentication to mount a Bryck
            no_enc: disable Data encryption
            no_erase: disable erase when formatting the Bryck
            raid_chunk: Chunk size to be used for raid setup
            raid_level: Raid level to be used for raid setup
            key_file: the file that contains the secret key
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        debug("Formatting the Bryck enc:{} erase:{} raid_chunk:{}"
              "raid_level:{} key_file:{}".format(not no_enc, not no_erase,
                                                 raid_chunk, raid_level,
                                                 key_file))
        # Skip formatting if Bryck is not found
        if not self.drives:
            return 1, self.messages['bryck_not_found']

        # Skip formatting if Bryck is mounted and in use
        if self.is_mounted():
            return 1, self.messages['bryck_format_err_mounted']

        self.serial_number = self.drives[0]['SerialNumber']
        self.format_time = str(datetime.now()) + " " + tzname[1]

        drive_names = self.get_drive_names()
        # skip erasing if specified
        info("Resetting the Bryck")
        debug("Resetting raid and encryption on Bryck drives")
        data_protection_reset(self.config['data_drive_name'], self.get_partition_names("data", drive_names))
        data_protection_reset(self.config['metadata_drive_name'], self.get_partition_names("meta", drive_names))
        partition_reset_drives(self.get_encrypt_drive_names(drive_names))
        encrypt_reset_drives(drive_names)

        if not no_erase:
            debug("Secure erasing Bryck drives")
            if self.config['drive_type'] == 'NVME':
                rc, out, err = nvme_erase_drives(self.drives)
            else:
                rc, out, err = sata_erase_drives(self.drives)
            if rc:
                return 1, self.messages['bryck_format_err_erase'] + err

        # setup encryption on Bryck drives
        if not no_enc:
            info("Setting up the encryption")
            if not key_file:
                return 1, self.messages['bryck_format_err_nokeyfile']
            rc, out, msg = encrypt_setup_drives(drive_names, key_file)
            if rc:
                return 1, self.messages['bryck_format_err_encryption'] + msg
            rc, out, msg,err_dives = encrypt_unlock_drives(drive_names, key_file)
            if rc:
                return 1, self.messages['bryck_format_err_unlock'] + msg
            drive_names = self.get_encrypt_drive_names(drive_names)

        info("Setting up the partitions")
        rc, msg = self.create_partitions(drive_names)
        if rc:
            return 1, self.messages['bryck_format_err_partition'] + msg

        data_partitions = self.get_partition_names("data", drive_names)
        metadata_partitions = self.get_partition_names("meta", drive_names)

        # Setup data protection on Bryck partitions
        info("Setting up the data protection")
        debug("Setting up the data protection for metadata")
        rc, out, err = data_protection_setup(
            metadata_partitions, self.config['metadata_drive_name'], 1)
        if rc:
            return 1, self.messages['bryck_format_err_metaraid'] + err

        debug("Setting up the data protection for data")
        rc, out, err = data_protection_setup(
            data_partitions, self.config['data_drive_name'], raid_level)
        if rc:
            return 1, self.messages['bryck_format_err_dataraid'] + err

        debug("Setting up the data file system")
        info("Setting up the file systems")
        # setup file systems on the data partition
        rc, out, err = filesystem_create(self.config['data_drive_name'])
        if rc:
            return 1, self.messages['bryck_format_err_data_fs'] + err

        # setup file systems on the metadata partitions
        debug("Setting up the meta data file system")
        rc, out, err = filesystem_create(self.config['metadata_drive_name'])
        if rc:
            return 1, self.messages['bryck_format_err_metadata_fs'] + err

        # Write metadata to the metadata file system
        info("Writing meta data")
        rc, msg = self.write_metadata()
        if rc:
            return 1, msg

        #eject the formatted Bryck
        rc, msg = self.eject()
        if rc:
            return rc, msg
        return 0, self.messages['bryck_format_success']

    def mount(self, key_file=None, mount_dir=None):
        """ Formats the Bryck and sets up a mountable encrypted file system
        Args:
            key_file: Key file to use authenticate/encrypt the Bryck
            mount_dir: Directory where the Bryck is to be mounted
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        """ Formats the Bryck and sets up a mountable encrypted file system
        Args:
            key_file: Key file to use authenticate/encrypt the Bryck
            mount_dir: Directory where the Bryck is to be mounted
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        debug("Bryck Mounting key_file:{} mount_dir:{}".format(key_file, mount_dir))

        #Bryck is inserted or not
        debug("Checking Bryck is inserted or not")
        if not self.drives:
            return 1, self.messages['bryck_not_found']

        if self.is_mounted():
            return 1, self.messages['bryck_mount_already']

        #Mount directory path provided or not
        debug("Checking mount directory is given")
        if mount_dir is None:
            return 1, self.messages['bryck_mount_err_nomount_dir']

        #If provided checking it exists or not
        if self.path_exists(mount_dir):
            return 1,self.messages['bryck_mount_err_mount_dir']

        #Check for encrption of drives
        debug("Checking Bryck is encrypted or not")
        drives = self.get_drive_names()
        if(key_file is None):  #Key file provided or not
            return 1, self.messages['bryck_mount_err_nokey_file']

        info("Bryck is encrypted and unlocking it")
        lock, stdout, stderr, errdrives = encrypt_unlock_drives(drives,key_file)
        if lock:
            if "Failed to open key file" in stderr or "No key available with this passphrase." in stderr:
                return 1, self.messages['bryck_mount_err_unlock_fail'] + ": wrong key provided"

            info("Found corrupted encrypted drives : " + ','.join(errdrives))
        else:
            info("Unlocked the drive successfully")

        #Reconstruct of metadata
        debug("Reconstruction of metadata")
        rc, err,out = data_protection_start()

        if rc:
            return 1, self.messages['bryck_mount_err_metadata']
        info("Metadata reconstructed succesfully")

        #Metadata path exists
        debug("Checking Metadata exists : ")
        debug(self.path_exists(self.config['metadata_drive_name']))
        if self.path_exists(self.config['metadata_drive_name']):
            return 1, self.messages['bryck_mount_err_metadata_path']

        debug("Mounting Meta data")
        rc, err, out = filesystem_mount(self.config['metadata_drive_name'],
                                        self.config['metadata_mount'],
                                        create=True)
        if rc:
            return 1,self.messages['bryck_mount_err_metadata']
        info("Mounted Meta data successfully")

        if lock:
            rc,msg = encrypt_recovery(self.config['metadata_mount'] +
                             self.config['backup_dir'],errdrives,key_file)
            if rc:
                return 1, self.messages['bryck_mount_err_unlock_fail']
            info("Recovered corrupted encrypted drives successfully")
            self.redo_mount()

        encrypt_drives = self.get_encrypt_drive_names(drives)
        errdrives = []
        for drive in encrypt_drives:
            cmd = "ls " + drive + "p*"
            rc,msg,err = run_cmd(cmd)
            if rc:
                errdrives.append(drive)

        if len(errdrives)>0:
            info("Found corrupted partitions : " + ','.join(errdrives))
            rc,msg = partition_recovery(self.config['metadata_mount'] +
                               self.config['backup_dir'],errdrives)
            if rc:
                return 1, self.messages['bryck_mount_err_partition_fail']
            info("Recovered corrupted partition successfully")
            self.redo_mount()

        #Data path exists
        debug("Checking data path exists")
        debug(self.path_exists(self.config['data_drive_name']))
        if self.path_exists(self.config['data_drive_name']):
            return 1,self.messages['bryck_mount_err_data_path']

        debug("Mounting data partition")
        rc, err, out = filesystem_mount(self.config['data_drive_name'],
                                        mount_dir,
                                        create=True)
        if rc:
            return 1,self.messages['bryck_mount_err_data']
        info("Mounted data partition successfully")

        #Unmounting metadata
        """
        #Don't unmount in agylagent
        debug("Unmounting meta data")
        rc, err, out = filesystem_unmount(self.config['metadata_mount'])
        if rc:
            return 1,self.messages['bryck_unmount_err_data']
        info("Unmounted metadata") """

        #Bryck Mounted successfully
        info("Mounting the Bryck " + mount_dir)
        return 0, self.messages['bryck_mount_success'] + mount_dir

    def path_exists(self, file_path):
        if path.exists(file_path):
            return 0
        else:
            return 1

    def is_ejected(self):
        # Check if the bryck mount point exists
        if self.is_mounted():
            return False

        #Check if the raid devices exist
        data_raid = data_protection_get_dev(self.config['data_drive_name'])
        metadata_raid = data_protection_get_dev(self.config['metadata_drive_name'])
        if data_raid or metadata_raid:
            return False

        #Check if the encrypt device exists
        for enc_drive in self.get_encrypt_drive_names(self.get_drive_names()):
            if exists(enc_drive):
                return False

        return True


    def eject(self):
        """ Eject the Bryck by gracefully unmounting and stopping raid
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        info("Ejecting the Bryck")

        if not self.drives:
            return 1, self.messages['bryck_not_found']

        if self.is_ejected():
            return 1, self.messages['bryck_eject_err_ejected']

        # if the Bryck is already mounted, flush all the cached data to disk
        # and then unmount it
        if self.is_mounted():
            #Unmouting metadata
            info("Unmounting metadata")
            rc, out, err = filesystem_unmount(self.config['metadata_mount'])
            if rc:
                return 1, self.messages['bryck_eject_err_unmount']
            info("Flushing cached data to Bryck")
            rc, out, err = filesystem_flush()
            if rc:
                return 1, self.messages['bryck_eject_err_flush']
            debug("Unmounting the Bryck")
            rc, out, err = filesystem_unmount(self.config['data_drive_name'])
            if rc:
                return 1, self.messages['bryck_eject_err_unmount']

        # Stop the raid
        debug("Stopping the raid")
        rc, out, err = data_protection_stop(self.config['data_drive_name'])
        if rc:
            return 1, self.messages['bryck_eject_err_stop_raid']

        rc, out, err = data_protection_stop(self.config['metadata_drive_name'])
        if rc:
            return 1, self.messages['bryck_eject_err_stop_raid']

        # Check if any encrypted Bryck drive exits

        # close the encryption disks
        info("Locking the Bryck")
        rc, out, err = encrypt_lock_drives(self.get_drive_names())
        if rc:
            return 1, self.messages['bryck_eject_err_lock'] + err

        return 0, self.messages['bryck_eject_success']

    def erase(self):
        """ Erase the Bryck data successfully
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        debug("Erasing the Bryck")
        if not self.drives:
            return 1, self.messages['bryck_not_found']

        if self.is_mounted():
            return 1, self.messages['bryck_erase_err_mounted']


        #ejecting the Bryck
        self.eject()
        if self.config['drive_type'] == 'NVME':
            rc, out, err = nvme_erase_drives(self.get_drive_names())
        else:
            rc, out, err = sata_erase_drives(self.get_drive_names())
        if rc:
            return 1, self.messages['bryck_erase_failed'] + "\n"+err

        return 0, self.messages['bryck_erase_success']

    def list(self, return_json=True):
        """ list the Bryck data successfully
        Returns:
             return code and drives list  as tuple
             return code: 0 for success and non-zero for failure
             drives list: return the list in json format
        """
        debug("Erasing the Bryck")
        list = {}
        list['device-list'] = []
        if not self.drives:
            return 1, self.messages['bryck_not_found']
        for drive in self.drives:
            drives_dict = {}
            drives_dict['model'] = drive['ModelNumber']
            drives_dict['serial'] = drive['SerialNumber']
            drives_dict['size'] = drive['PhysicalSize']
            drives_dict['name'] = drive['DevicePath']
            drives_dict['subsystems'] = "block:scsi:pci"
            list['device-list'].append(drives_dict)

        if return_json:
            return_list = json.dumps(list)
        else:
            return_list = list
        return 0, return_list

    def setkey(self, old_key=None, new_key=None):
        """ Change the secret key used for authentication and encryption
        Args:
            old_key: file path of the old key
            new_key: file path of the new key
        Returns:
             return code and message as tuple
             return code: 0 for success and non-zero for failure
             message: Description to reflect the return code
        """
        debug("Changing the key")
        if not self.drives:
            return 1, self.messages['bryck_not_found']

        if not encrypt_is_enabled(self.get_drive_names()):
            return 1, self.messages['bryck_setkey_err_no_encryption']

        if not old_key:
            return 1, self.messages['bryck_setkey_err_no_oldkey']

        if not new_key:
            return 1, self.messages['bryck_setkey_err_no_newkey']

        info("Setting the new key")
        rc, out, err = encrypt_change_key_drives(self.get_drive_names(),
                                                       old_key, new_key)
        if rc:
            return 1, self.messages['bryck_setkey_err_failed']+"\n"+err

        return 0, self.messages['bryck_setkey_success']

    def info(self):
        """ return the information about the Bryck
        Returns:
             return code and message as tuple
             return code: 0 if the bryck is inserted and
                          1 if the bryck is not found
             message: Description to reflect the return code
        """
        debug("Getting bryck information")
        if not self.drives:
            return 1, self.messages['bryck_not_found']

        out = "ProductName: {}\n".format(self.product_name)
        out += "SerialNumber: {}\n".format(self.serial_number)
        out += "FirmwareRev: {}\n".format(self.firmware_rev)
        out += "Capacity: {} GB\n".format(self.raw_capacity)
        out += "DevicePath: {}\n".format(self.config['data_drive_name'])
        if not self.is_mounted():
            return 0, out

        rc, fsdata = filesystem_usage(self.config['data_drive_name'])
        if fsdata:
            out += "UsableCapacity: {} GB\n".format(fsdata['usable_capacity'])
            out += "UsedSpace: {} GB\n".format(fsdata['used_space'])
            out += "AvailableSpace: {} GB\n".format(fsdata['available_space'])
            out += "Usage: {} GB\n".format(fsdata['usage'])
        else:
            out += self.messages['bryck_info_unable_df']
        return 0, out

    def get_drives(self):
        """ Returns all drive paths for all Bryck drives in a list
        Returns:
            list of drives if the Bryck found and
            empty list if the Bryck is not found
        """
        debug("Enumerate all Bryck drives")
        bryck_drives = []
        if self.config['drive_type'] == 'NVME':
            rc, drives = nvme_list_drives()
        else:
            rc, drives = sata_list_drives()

        if rc:
            debug("No Bryck drives found")
            drives = []
        for drive in drives:
            if self.config['drive_type'] == 'NVME':
                if drive['ModelNumber'] in self.config['bryck_drive_model'] and \
                 "c1" not in drive['DevicePath']:
                    bryck_drives.append(drive)
            else:
                if drive['model'] == "Samsung SSD 850 ":
                    sata_drives = {}
                    sata_drives['ModelNumber'] = drive['model']
                    sata_drives['SerialNumber'] = drive['serial']
                    sata_drives['PhysicalSize'] = drive['size']
                    sata_drives['DevicePath'] = '/dev/'+drive['kname']
                    bryck_drives.append(sata_drives)
        return bryck_drives

    def get_drive_info(self, drive):
        """ Return the information about a Bryck drive
        Args:
            drive: path of a Bryck drive
        Returns:
            drive information as set of key value parameters
        """
        return 0, {}

    def redo_mount(self):
        filesystem_unmount(self.config['metadata_mount'])
        data_protection_stop(self.config['metadata_mount'])
        data_protection_stop(self.config['data_drive_name'])
        data_protection_start()
        filesystem_mount(self.config['metadata_drive_name'],
                         self.config['metadata_mount'],
                         create=True)
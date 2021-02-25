# !/usr/bin/env python
from metastream import MetaStream
from bryckrecovery import BryckRecovery
from libutils import run_cmd

from datetime import datetime
from json import load
from os.path import dirname
import os

config = dirname(__file__) + "/config.json"
with open(config) as cfg:
    config = load(cfg)

class PartStream(MetaStream):
    def __init__(self,id,type,desc=None,filename=""):
        super().__init__()
        self.create_stream(id=id,type=type,desc=None,filename=filename)
        self.filename = filename

    def backup_header(self,drive_name):
        file_name = config['metadata_mount'] + \
                    config['backup_dir'] + "part_drives/" + \
                    datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + ".bin"
        cmd = "sudo sfdisk -d " + drive_name + " > " + file_name
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        if rc:
            return 1, "partition backup failed"
        return self.append_chunk('partition', drive_name, file_name)

class PartRecovery(BryckRecovery):
    def __init__(self):
        super().__init__()

    def restore_header(self,drive_name):
        file_name = self.stream['partition']['data'][drive_name]
        cmd = "sudo sfdisk --force " + drive_name + " < " + file_name
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        cmd = "sudo partprobe"
        run_cmd(cmd)
        if rc:
            return 1, "Failed to restore the partition drive"
        return 0, "Successfully recovered"

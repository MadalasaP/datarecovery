# !/usr/bin/env python
from metastream import MetaStream
from bryckrecovery import BryckRecovery
from libutils import run_cmd

from datetime import datetime
from json import load
from os.path import dirname

config = dirname(__file__) + "/config.json"
with open(config) as cfg:
    config = load(cfg)

class PartStream(MetaStream):
    def __init__(self,id,type,desc=None,filename=""):
        super().__init__()
        self.create_stream(id=id,type=type,desc=None,filename=filename)
        self.filename = filename

    def backup_header(self,drive_name):
        cmd = "sudo sfdisk -d " + drive_name + " > back_up_filename"\
                   + config['tmp_dir'] + "tmp.bin"
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        if rc:
            return 1, "Partition backup failed"
        try:
            f = open(config['tmp_dir'] + "tmp.bin", "r")
            data = f.read()
        except:
            return  1, "ENC backup failed"
        cmd = "sudo rm " + config['tmp_dir'] + "tmp.bin"
        rc, msg, err = run_cmd(cmd)
        return self.append_chunk('partition',drive_name,data)

class PartRecovery(BryckRecovery):
    def __init__(self):
        super().__init__()

    def persist_tmp_file(self,header):
        try:
            f = open(config['tmp_dir'] + "tmp.bin", "w")
            f.write(header)
            f.close()
            return 0,config['tmp_dir'] + "tmp.bin"
        except:
            return 1, "Fail_err"

    def restore_header(self,drive_name):
        header = self.stream['partition']['data'][drive_name]
        print(header)
        rc,file_name = self.persist_tmp_file(header)
        cmd = "sudo sfdisk " + drive_name + " < " + file_name
        rc, msg, err = run_cmd(cmd)
        print(msg)
        if rc:
            return 1, "ENC backup failed"
        # cmd = " fdisk -l " + drive_name
        if not rc:
            return 0, "Successfully recovered"
        return 1, "Failed to restore from recovery"

if __name__ == "__main__":
    file = config['metadata_backup_dir'] + "partition" + str(datetime.now()) + ".bin"
    part_stream = PartStream("1234","partition",desc=None,filename=file)
    rc,msg = part_stream.backup_header("/dev/mapper/cryptnvme0n1l")
    print(msg)
    if not rc:
        rc,msg = part_stream.persist("partition")
        print(msg)

    part_rec = PartRecovery()
    suc_count, err_count, err_files, types = part_rec.read_streams(config['metadata_backup_dir'])
    print(err_files)
    print(types)
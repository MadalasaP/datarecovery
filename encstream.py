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

class EncStream(MetaStream):
    def __init__(self,id,type,desc=None,filename=""):
        super().__init__()
        self.create_stream(id=id,type=type,desc=None,filename=filename)
        self.filename = filename

    def backup_header(self,drive_name):
        file_name = config['metadata_mount'] + \
                    config['backup_dir'] + "enc_drives/" + \
                    datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + ".bin"
        cmd = "sudo cryptsetup luksHeaderBackup " + \
              drive_name + " --header-backup-file " + file_name
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        if rc:
            return 1, "ENC backup failed"
        # cmd = "sudo cryptsetup luksDump " + config['tmp_dir'] + "tmp.bin"
        # rc,msg,err = run_cmd(cmd)
        # print(cmd)
        # cmd = "sudo rm " + config['tmp_dir'] + "tmp.bin"
        # tmp = run_cmd(cmd)
        # rc, file = persist_tmp_file(msg)
        # print(file)
        # cmd = "sudo cryptsetup luksDump " + file
        # rc, msg, err = run_cmd(cmd)
        # print(cmd)
        # print(rc)
        # print(msg)
        # if rc or msg=="":
        #     return  1, "ENC backup failed"
        # cmd = "sudo rm " + config['tmp_dir'] + "tmp.bin"
        # tmp = run_cmd(cmd)
        return self.append_chunk('encryption', drive_name, file_name)


# def persist_tmp_file(header):
#     try:
#         f = open(config['tmp_dir'] + "tmp.bin", "w")
#         n = f.write(header)
#         print(header)
#         f.close()
#         return 0, config['tmp_dir'] + "tmp.bin"
#     except:
#         return 1, "Fail_err"


class EncRecovery(BryckRecovery):
    def __init__(self):
        super().__init__()

    def restore_header(self,drive_name):
        file_name = self.stream['encryption']['data'][drive_name]
        # rc,file_name = persist_tmp_file(header)
        # if rc:
        #     return 1, "File Corrupted"
        cmd = "sudo cryptsetup luksDump " + file_name
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        if rc:
            return 1, "ENC backup failed"
        cmd = "yes | sudo cryptsetup luksHeaderRestore " + drive_name + " --header-backup-file " + file_name
        print(cmd)
        rc, msg, err = run_cmd(cmd)
        print(msg)
        print(err)
        # cmd = "sudo rm " + config['tmp_dir'] + "tmp.bin"
        # rc, msg, err = run_cmd(cmd)
        if not rc:
            return 0, "Successfully recovered"
        return 1, "Failed to restore from recovery"

# if __name__ == "__main__":
#     # file = config['metadata_backup_dir'] + "encryption" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + ".bin"
#     # enc_stream = EncStream("1234","encryption",desc=None,filename=file)
#     # rc,msg = enc_stream.backup_header("/dev/nvme0n1")
#     # print(msg)
#     # rc, msg = enc_stream.backup_header("/dev/nvme0n2")
#     # print(msg)
#     # rc, msg = enc_stream.backup_header("/dev/nvme0n3")
#     # print(msg)
#     # if not rc:
#     #     rc,msg = enc_stream.persist("encryption")
#     #     print(msg)
#
#     enc_rec = EncRecovery()
#     suc_count, err_count, err_files, types = enc_rec.read_streams(config['metadata_backup_dir'])
#     print(err_files)
#     print(types)
#     print("encryption" in types)
#     if "encryption" in types:
#         rc, msg = enc_rec.restore_header("/dev/nvme0n1")
#         print(msg)
#         rc, msg = enc_rec.restore_header("/dev/nvme0n2")
#         print(msg)
#         rc,msg = enc_rec.restore_header("/dev/nvme0n3")
#         print(msg)
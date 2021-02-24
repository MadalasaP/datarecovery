# !/usr/bin/env python
from metastream import MetaStream

from json import dumps, loads, load
from json.decoder import JSONDecodeError
from datetime import datetime
import hashlib
import base64
import sys
import os

class BryckRecovery(MetaStream):
        def __init__(self):
            super().__init__()

        def read_streams(self, dir_location):
            err_files = []
            err_count = 0
            suc_count = 0
            types = []
            files = os.listdir(dir_location)
            for file in files:
                if os.path.isfile(dir_location + file):
                    rc, msg = self.get_type(dir_location + file)
                    if rc:
                        err_files.append(dir_location + file)
                        err_count += 1
                    else:
                        suc_count += 1
                        types.append(msg[0])
            return suc_count, err_count, err_files, types

        def get_type(self, file_name):
            print(file_name)
            f = open(file_name)
            data_bytes = f.read()
            f.close()
            return self.decode_stream(data_bytes)

        def decode_stream(self, data_bytes):
            data_bytes = base64.b64decode(data_bytes)
            ascii_stream = data_bytes.decode('ascii')
            ascii_stream = ascii_stream.replace("'", "\"")
            try:
                data = loads(ascii_stream)
                if not self.validate(list(data.keys())[0],data):
                    return 1, self.msg['file_corrupt']
                self.stream.update(data)
                return 0, list(data.keys())
            except JSONDecodeError as err:
                return 1, self.msg['file_corrupt']

        def validate(self,stream_type,data):
            org_checksum = data[stream_type]['checksum']
            del data[stream_type]['checksum']
            hasher = hashlib.md5()
            hasher.update(dumps(data,sort_keys=True).encode('utf-8'))
            checksum = hasher.hexdigest()
            if checksum == org_checksum:
                return True
            return False


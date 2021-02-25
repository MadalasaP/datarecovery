# !/usr/bin/env python
from json import dumps, loads, load
from datetime import datetime
from os.path import dirname
import hashlib
import base64
import sys
import os

class MetaStream:
    def __init__(self):
        self.stream = {}
        msg_file = dirname(__file__) + "/messages.json"
        with open(msg_file) as msg:
            self.msg = load(msg)

    def create_stream(self,id,type,desc=None,filename=""):
        self.stream[type] = {'id':id,'description':desc,'filename':filename,'data':{}}

    def append_chunk(self,stream_type,key,value):
        if stream_type in self.stream.keys():
            self.stream[stream_type]['data'][key]=value
            return 0, self.msg['Chunk_add']
        return 1,type + self.msg['Stream_err']

    def read_chunk(self,stream_type,key):
        if stream_type in self.stream.keys():
            if key in self.stream[stream_type]['data'].keys():
                return 0, self.stream[stream_type]['data'][key]
            return 1, key + self.msg['Chunk_err']
        return 1, type + self.msg['Stream_err']

    def delete_chunk(self, stream_type, key):
        if stream_type in self.stream.keys():
            if key in self.stream[stream_type]['data'].keys():
                del self.stream[stream_type]['data'][key]
                return 0, self.msg['Chunk_del']
            return 0, key + self.msg['Chunk_no_del']
        return 1, type + self.msg['Stream_err']

    def dump_stream(self,stream_type):
        return self.stream[stream_type]

    def persist(self,stream_type,enc_key=None):
        bytes = self.encode_stream(stream_type)
        print(print(self.stream[stream_type]['filename']))
        try:
            f = open(self.stream[stream_type]['filename'], "wb")
            print(self.stream[stream_type]['filename'])
            f.write(bytes)
            f.close()
            rc,msg = self.delete_stream(stream_type)
            return 0, self.msg['persist']
        except IOError as e:
            return 1, self.msg['persist_err']

    def encode_stream(self,stream_type):
        hasher = hashlib.md5()
        hasher.update(dumps(self.stream,sort_keys=True).encode('utf-8'))
        self.stream[stream_type]['checksum'] = hasher.hexdigest()
        ascii_stream = dumps(self.stream,sort_keys=True).encode('ascii')
        bytes = base64.b64encode(ascii_stream)
        return bytes

    def delete_stream(self,stream_type):
        if stream_type in self.stream.keys():
            del self.stream[stream_type]
            return 0, self.msg['Stream_del']
        return 0, stream_type + self.msg['Stream_no_del']

    def read_stream(self,stream_type):
        if stream_type in self.stream.keys():
            return 0,self.stream[stream_type]
        return 1, self.msg['Stream_not_exist']

    def dump_chunk(self,stream_type,drive_name):
        if len(self.stream)>0:
            return 0,self.stream[stream_type]['data'][drive_name]
        return 1, self.msg['Stream_not_exist']
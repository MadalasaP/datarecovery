# !/usr/bin/env python

from metastream import MetaStream
from bryckrecovery import BryckRecovery

from datetime import datetime
from json import load
import random
import string
import os

with open("config.json") as cfg:
    config = load(cfg)

def generate_string(length):
    res =  ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for i in range(length))
    return res

def generate_binary(l_range,u_range):
    return bin(random.randint(l_range,u_range))

def invoke_error():
    file_name = config["metadata_backup_dir"] + random.choice(os.listdir(config["metadata_backup_dir"]))
    print("Invoked error in %s file",file_name)
    f = open(file_name,"rt")
    data = f.read()
    data = data.replace('A','Z')
    f.close()
    f = open(file_name,"wt")
    f.write(data)
    f.close()
    return file_name

def add_streams(id,type,data_type,chunks,key_len,chunk_len,chunk_len_up=1000000):
    stream = MetaStream()
    print("Adding Streams : " + type)
    filename = config["metadata_backup_dir"] + type + str(datetime.now()) + '.bin'
    stream.create_stream(id=id, type=type,filename=filename)
    # Add chunks
    if data_type == 'binary':
        for i in range(100):
            rc, msg = stream.append_chunk('filesystem',
                                          generate_string(key_len),
                                          generate_binary(chunk_len, chunk_len_up))
            # print(msg)
    else:
        for i in range(chunks):
            rc, msg = stream.append_chunk(type,
                                          generate_string(key_len), generate_string(chunk_len))
            # print(msg)

    # Dump
    # stream.dump_stream(type)

    # Persist
    stream.persist(type)


def data_recovery():
    print("Recovering")
    recovery = BryckRecovery()
    suc_count, err_count, err_files, types = recovery.read_streams(config['metadata_backup_dir'])
    # print("Erroneous file count " + str(err_count))
    # print("Error files : ")
    # print('\\n'.join(err_files))
    # print("Stream types are : ")
    # print(types)

    # for type in types:
    #     recovery.dump_stream(list(type)[0])
    return err_files

if __name__ == "__main__":
    print("Testing starts")
    files = []

    for i in range(20):
        add_streams('1234',generate_string(4),random.choice(''.join('txt'+'binary')),100,10,20000)

    for i in range(10):
        if len(data_recovery())>0:
            print("Recovered Failure")
        else:
            print("Recovered Success")

    for i in range(10):
        files.append(invoke_error())

    err_files = data_recovery()

    files = (list(set(files)))
    files.sort()
    err_files.sort()

    if files == err_files:
        print("Found all error files successfully")
    else:
        print("Failed in finding error files")
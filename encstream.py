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


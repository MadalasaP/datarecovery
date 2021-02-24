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

import click
from tsulib import Bryck, get_version, get_tsutil_name, set_log_level
import sys


class Options(object):
    """ Global options for the CLI commands"""

    def __init__(self, verbose=False,json=False):
        self.verbose = verbose
        self.json = json


# Command group for bryck management
@click.group()
def bryck():
    """ Bryck Management"""
    pass


# Command group for system management
@click.group()
def system():
    """ System Management """
    pass


# Command group for CLI
@click.group()
@click.pass_context
@click.option("--verbose", is_flag=True, help="Enable verbose logs")
@click.option("--json", is_flag=True, help="Enable output in JSON format")
@click.version_option(get_version(), prog_name=get_tsutil_name())
def cli(ctx, verbose,json):
    ctx.obj = Options(verbose,json)
    pass


# Bryck format sub command definition
@bryck.command()
@click.pass_obj
@click.option("--no_enc", is_flag=True, help="Disable data encryption")
@click.option("--key_file", type=click.Path(exists=True),
              help="Secret key file")
@click.option("--no_erase", is_flag=True, help="Disable secure erasing of data")
@click.option("--raid_chunk", default=1,
              help="Raid chunk size in MBs. Default 1MB")
@click.option("--raid_level", type=click.Choice(['0', '5', '6']), default='5',
              help="Raid level for data protection. Default 5")
@click.confirmation_option(prompt='\n**Formatting deletes Bryck content' +
                                  'permanently**\n**Do you want to continue?')
def format(obj, no_enc, no_erase, raid_chunk, raid_level, key_file):
    """ Format the Bryck """
    set_log_level(obj.verbose)
    rc, msg = Bryck(obj.verbose).format(no_auth=False, no_enc=no_enc,
                                        no_erase=no_erase,
                                        raid_chunk=raid_chunk,
                                        raid_level=int(raid_level),
                                        key_file=key_file)
    if not rc:
        msg = "Bryck formatted"
    click.echo(msg)
    sys.exit(rc)


# Bryck mount sub command definition
@bryck.command()
@click.pass_obj
@click.option("--key_file", type=click.Path(), help="Secret key file")
@click.argument("mount_dir", type=click.Path(exists=True))
def mount(obj, key_file, mount_dir):
    """ Mount the Bryck """
    set_log_level(obj.verbose)
    rc, msg = Bryck(obj.verbose).mount(key_file=key_file, mount_dir=mount_dir)
    if not rc:
        msg = "Bryck mounted"
    click.echo(msg)
    sys.exit(rc)


# Bryck eject sub command definition
@bryck.command()
@click.pass_obj
def eject(obj):
    """ Eject the Bryck """
    set_log_level(obj.verbose)
    rc, msg = Bryck(obj.verbose).eject()
    if not rc:
        msg = "Bryck ejected"
    click.echo(msg)
    sys.exit(rc)


# Bryck eject sub command definition
@bryck.command()
@click.pass_obj
@click.confirmation_option(prompt='\n**Erasing deletes Bryck content ' +
                                  'permanently**\n**Do you want to continue?')
def erase(obj):
    """ Erase the Bryck data """
    set_log_level(obj.verbose)
    rc, msg = Bryck(obj.verbose).erase()
    if not rc:
        msg = "Bryck erased"
    click.echo(msg)
    sys.exit(rc)


# Bryck setkey sub command definition
@bryck.command()
@click.pass_obj
@click.argument("old_key", type=click.Path(exists=True))
@click.argument("new_key", type=click.Path(exists=True))
def setkey(obj, old_key, new_key):
    """ Change the encryption key """
    set_log_level(obj.verbose)
    rc, msg = Bryck(obj.verbose).setkey(old_key=old_key, new_key=new_key)
    if not rc:
        msg = "Secret key changed"
    click.echo(msg)
    sys.exit(rc)


# Bryck info sub command definition
@bryck.command()
@click.pass_obj
def info(obj):
    """ Display Bryck information"""
    set_log_level(obj.verbose)
    rc, binfo = Bryck(obj.verbose).info()
    if rc:
        click.echo("Bryck not found")
    else:
        if(obj.json):
            """ Print in JSON format"""
            binfo = dict(map(lambda x: x.split(':',1), binfo.split('\n')[:-1]))
        click.echo(binfo)
    sys.exit(rc)


# Bryck list sub command definition
@bryck.command()
@click.pass_obj
def list(obj):
    """ Display Bryck information"""
    set_log_level(obj.verbose)
    rc, binfo = Bryck(obj.verbose).list()
    if rc:
        click.echo("Bryck not found")
    else:
        click.echo(binfo)
    sys.exit(rc)


def build_cli():
    """ Builds CLI sub commands"""
    cli.add_command(bryck)
    cli.add_command(system)


def run_cli():
    """ Runs the CLI"""
    build_cli()
    cli()

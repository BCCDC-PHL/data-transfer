#!/usr/bin/env python

import argparse
import itertools
import json
import logging
import pwd
import socket
import subprocess
import sys
import time
import multiprocessing
import os     

from datetime import datetime

import paramiko


def sync(transfer_dir, src, dest):
    logging.info(os.path.join(dest, transfer_dir))
    transfer_start_time = datetime.now().astimezone().isoformat()

    subprocess.call(["rsync", "-aq", os.path.join(src, transfer_dir), dest])
    transfer_complete_time = datetime.now().astimezone().isoformat()
    local_hostname = socket.gethostname()
    if ':' in src:
        src_hostname, src_dir = src.split(':') 
        src = ':'.join([src_hostname, os.path.join(src, transfer_dir)])
    else:
        src = ':'.join([local_hostname, os.path.abspath(os.path.join(src, transfer_dir))])

    transfer_complete = {
        "operation": "data_migration",
        "src": src,
        "dest": ':'.join([local_hostname, os.path.abspath(os.path.join(dest, transfer_dir))]),
        "timestamp_transfer_start": transfer_start_time,
        "timestamp_transfer_complete": transfer_complete_time,
    }

    with open(os.path.join(dest, transfer_dir, 'transfer_complete.json'), 'w') as f:
        json.dump(transfer_complete, f, indent=2)
        f.write('\n')


def list_dirs_remote(src):
    src_hostname = src.split(':')[0]
    src_dir = src.split(':')[1]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    username = pwd.getpwuid(os.getuid())[0]
    key_path = os.path.expanduser('~/.ssh/id_rsa')
    client.connect(hostname=src_hostname, username=username, key_filename=os.path.expanduser('~/.ssh/id_rsa') )
    command = ' '.join(['ls', '-1', src_dir])
    stdin , stdout, stderr = client.exec_command(command)
    transfer_dirs = stdout.read().decode('UTF-8').split('\n')
    transfer_dirs = list(filter(lambda x: x != "", transfer_dirs))

    return transfer_dirs


def list_dirs_local(src):
    transfer_dirs = []
    for d in os.listdir(src):
        if os.path.isdir(os.path.join(src, d)):
            transfer_dirs.append(d)

    return transfer_dirs


def main(args):
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s :: %(levelname)s] %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z")

    transfer_dirs = []
    
    if ':' in args.src:
        transfer_dirs = list_dirs_remote(args.src)
    else:
        transfer_dirs = list_dirs_local(args.src)

    if args.ascending:
        transfer_dirs = sorted(transfer_dirs, reverse=False)
    else:
        transfer_dirs = sorted(transfer_dirs, reverse=True)

    if args.before:
        transfer_dirs = list(filter(lambda x: x[0:len(args.before)] < args.before, transfer_dirs))

    if args.after:
        transfer_dirs = list(filter(lambda x: x[0:len(args.after)] > args.after, transfer_dirs))
        
    print(json.dumps(transfer_dirs, indent=2))
    exit(0)

    with multiprocessing.Pool(processes=args.processes) as pool:
        transfers = list(zip(transfer_dirs, itertools.cycle([args.src]), itertools.cycle([args.dest])))
        pool.starmap(sync, transfers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', type=int, default=4, help="Number of simultaneous transfers")
    parser.add_argument('-s', '--src', help="Source directory")
    parser.add_argument('-d', '--dest', help="Destination directory")
    parser.add_argument('-a', '--ascending', action='store_true', help="Transfer directories in ascending order by directory name (default is descending order)")
    parser.add_argument('--before', help="Transfer directories whose names are lexicographically before BEFORE")
    parser.add_argument('--after', help="Transfer directories whose names are lexicographically after AFTER")
    args = parser.parse_args()
    
    main(args)


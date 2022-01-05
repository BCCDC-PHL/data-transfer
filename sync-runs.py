#!/usr/bin/env python

import argparse
import itertools
import json
import logging
import pwd
import subprocess
import sys
import time
import multiprocessing
import os     

from datetime import datetime

import paramiko

def run_sync(run_dir, src, dest):
    logging.info(os.path.join(dest, run_dir))
    transfer_start_time = datetime.now().astimezone().isoformat()

    subprocess.call(["rsync", "-aq", os.path.join(src, run_dir), dest])
    transfer_complete_time = datetime.now().astimezone().isoformat()
    
    transfer_complete = {
        "operation": "data_migration",
        "src": src,
        "dest": os.path.join(dest, run_dir),
        "timestamp_transfer_start": transfer_start_time,
        "timestamp_transfer_complete": transfer_complete_time,
    }

    with open(os.path.join(dest, run_dir, 'transfer_complete.json'), 'w') as f:
        json.dump(transfer_complete, f, indent=2)
        f.write('\n')

def main(args):
    logging.basicConfig(level=logging.INFO, name=__name__, format='[%(asctime)s :: %(name)s :: %(levelname)s] %(message)s')

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host_keys = client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    src_hostname = args.src.split(':')[0]
    src_dir = args.src.split(':')[1]
    username = pwd.getpwuid(os.getuid())[0]
    key_path = os.path.expanduser('~/.ssh/id_rsa')
    print(key_path)

    client.connect(hostname=src_hostname, username=username, key_filename=os.path.expanduser('~/.ssh/id_rsa') )

    command = ' '.join(['ls', '-1', src_dir])
    stdin , stdout, stderr = client.exec_command(command)
    run_dirs = list(filter(lambda x: x != "", stdout.read().decode('UTF-8').split('\n')))
    
    with multiprocessing.Pool(processes=args.processes) as pool:
        transfers = list(zip(run_dirs, itertools.cycle([args.src]), itertools.cycle([args.dest])))
        pool.starmap(run_sync, transfers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', type=int, default=2)
    parser.add_argument('src')
    parser.add_argument('dest')
    args = parser.parse_args()
    
    main(args)


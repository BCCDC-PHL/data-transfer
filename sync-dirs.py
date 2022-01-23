#!/usr/bin/env python

import argparse
import datetime
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

from stat import S_ISDIR

import paramiko


def sync(transfer_dir, src, dest, username=None, key_path=None):
    logging.info("Transfer started: " + os.path.join(src, transfer_dir) + " -> " + os.path.abspath(os.path.join(dest, transfer_dir)))
    transfer_start_time = datetime.datetime.now().astimezone().isoformat()

    subprocess.call(["rsync", "-aq", os.path.join(src, transfer_dir), dest])
    transfer_complete_time = datetime.datetime.now().astimezone().isoformat()
    logging.info("Transfer complete: " + os.path.join(src, transfer_dir) + " -> " + os.path.abspath(os.path.join(dest, transfer_dir)))
    local_hostname = socket.gethostname()
    if ':' in src:
        src_hostname, src_dir = src.split(':') 
        src = ':'.join([src_hostname, os.path.join(src, transfer_dir)])
        #have to create a client object here in order to fetch size, as client object cannot be passed through multiprocessing
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if username is None:
            username = pwd.getpwuid(os.getuid())[0]
        if key_path is None:
            key_path = os.path.expanduser('~/.ssh/id_rsa')
        client.connect(hostname=src_hostname, username=username, key_filename=key_path)
        ftp = client.open_sftp()
        totalsize_src = getsize_src(os.path.join(src_dir,transfer_dir), ftp)
    else:
        #if files are transferred locally,then use the getsize local function 
        totalsize_src = getsize_dest(os.path.join(src,transfer_dir)) 
        src = ':'.join([local_hostname, os.path.abspath(os.path.join(src, transfer_dir))])
        
    
    totalsize_dest = getsize_dest(os.path.join(dest,transfer_dir))
    
    transfer_complete = {
        "operation": "data_migration",
        "src": src,
        "dest": ':'.join([local_hostname, os.path.abspath(os.path.join(dest, transfer_dir))]),
        "timestamp_transfer_start": transfer_start_time,
        "timestamp_transfer_complete": transfer_complete_time,
        "total_size_on_source_gb" : round(totalsize_src /1e9,5),
        "total_size_on_destination_gb" : round(totalsize_dest /1e9,5)
    }

    with open(os.path.join(dest, transfer_dir, 'transfer_complete.json'), 'w') as f:
        json.dump(transfer_complete, f, indent=2)
        f.write('\n')


def getsize_src(folderpath, ftp):
    folderattr =  ftp.listdir_attr(folderpath)
    size = 0
    for i in folderattr:
        mode = i.st_mode
        if S_ISDIR(mode):
            size += getsize_src(folderpath + "/" + i.filename, ftp)
        else:
            size += i.st_size
    return size


def getsize_dest(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def list_dirs_remote(src, username=None, key_path=None):
    src_hostname = src.split(':')[0]
    src_dir = src.split(':')[1]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if username is None:
        username = pwd.getpwuid(os.getuid())[0]
    if key_path is None:
        key_path = os.path.expanduser('~/.ssh/id_rsa')
    client.connect(hostname=src_hostname, username=username, key_filename=key_path)
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
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s :: %(levelname)s] %(message)s')
    logging.Formatter.formatTime = (lambda self, record, datefmt: datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc).astimezone().isoformat())

    transfer_dirs = []
    
    if ':' in args.src:
        transfer_dirs = list_dirs_remote(args.src, args.user, args.key)
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

    with multiprocessing.Pool(processes=args.processes) as pool:
        # Build a list of lists:
        # [
        #   [ run_dir_0, src_dir, dest_dir, username, ssh_key_path ],
        #   [ run_dir_1, src_dir, dest_dir, username, ssh_key_path ],
        #   [ run_dir_2, src_dir, dest_dir, username, ssh_key_path ],
        #   ...
        # ]
        transfers = list(zip(transfer_dirs, itertools.cycle([args.src]), itertools.cycle([args.dest]), itertools.cycle([args.user]), itertools.cycle([args.key])))
        pool.starmap(sync, transfers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', type=int, default=4, help="Number of simultaneous transfers")
    parser.add_argument('-s', '--src', help="Source directory")
    parser.add_argument('-d', '--dest', help="Destination directory")
    parser.add_argument('-u', '--user', help="Username")
    parser.add_argument('-k', '--key', help="SSH private key")
    parser.add_argument('-a', '--ascending', action='store_true', help="Transfer directories in ascending order by directory name (default is descending order)")
    parser.add_argument('--before', help="Transfer directories whose names are lexicographically before BEFORE")
    parser.add_argument('--after', help="Transfer directories whose names are lexicographically after AFTER")
    args = parser.parse_args()
    
    main(args)


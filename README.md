# Data Transfer Scripts

Scripts for assisting with data transfer between servers

## Usage

```
usage: sync-dirs.py [-h] [-p PROCESSES] [-s SRC] [-d DEST] [-u USER] [-k KEY] [-a] [--before BEFORE] [--after AFTER]

options:
  -h, --help            show this help message and exit
  -p PROCESSES, --processes PROCESSES
                        Number of simultaneous transfers
  -s SRC, --src SRC     Source directory
  -d DEST, --dest DEST  Destination directory
  -u USER, --user USER  Username
  -k KEY, --key KEY     SSH private key
  -a, --ascending       Transfer directories in ascending order by directory name (default is descending order)
  --before BEFORE       Transfer directories whose names are lexicographically before BEFORE
  --after AFTER         Transfer directories whose names are lexicographically after AFTER
```
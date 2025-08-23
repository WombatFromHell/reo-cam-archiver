#!/bin/sh
PYTHON=/usr/bin/python3
$PYTHON "/camera/archiver.py" --age 3 --cleanup

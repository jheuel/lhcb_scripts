#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os.path import join, basename
import glob
import subprocess
import argparse
from tqdm import tqdm


def merge(filepath):
    files = glob.glob(join(filepath, '*/*.root'))
    tuplename = basename(files[0])

    cmd = [
            'hadd', '-ff', '-n', '10',
            join(filepath, tuplename),
    ] + files

    tqdm.write(str(cmd))
    subprocess.call(cmd)


parser = argparse.ArgumentParser()
parser.add_argument('filepaths', nargs='+')
args = parser.parse_args()

for filepath in tqdm(args.filepaths, dynamic_ncols=True):
    merge(filepath)

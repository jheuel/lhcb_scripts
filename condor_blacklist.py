#!/usr/bin/env python
# -*- coding: utf-8 -*-

import htcondor
from os import utime
import argparse


def touch(fname, times=None):
    with open(fname, 'a'):
        utime(fname, times)


parser = argparse.ArgumentParser()
parser.add_argument('--release', action='store_true', help='Release jobs after blacklisting host')
parser.add_argument('--verbose', action='store_true', help='Print more stuff')
cfg = parser.parse_args()


schedd = htcondor.Schedd()
statusNumbers = {
    'Unexpanded': 0,
    'Idle': 1,
    'Running': 2,
    'Removed': 3,
    'Completed': 4,
    'Held': 5,
    'Submission_err': 6,
}

jobs = []
for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'JobStatus', 'HoldReason', 'LastRemoteHost', 'Requirements']):
    if job['JobStatus'] == statusNumbers['Held']:
        splits = job['LastRemoteHost'].split('@')
        if len(splits) != 2:
            print('could not parse host')
            exit(1)
        host = splits[1]
        req = job['Requirements']
        newRequirements = '{req} && (machine != "{host}")'.format(
            req=req,
            host=host)

        if cfg.verbose:
            print('old requirements: {}'.format(req))
            print('new requirements: {}'.format(newRequirements))

        j = ['{}.{}'.format(job['ClusterId'], job['ProcId'])]
        with schedd.transaction() as txn:
            schedd.edit(j, 'Requirements', newRequirements)
        jobs += j

if cfg.release:
    with schedd.transaction() as txn:
        schedd.act(htcondor.JobAction.Release, jobs)

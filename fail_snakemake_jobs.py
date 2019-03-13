#!/usr/bin/env python
# -*- coding: utf-8 -*-

import htcondor
import re
from os import utime


def touch(fname, times=None):
    with open(fname, 'a'):
        utime(fname, times)


schedd = htcondor.Schedd()

failFileRegex = re.compile(r'\|\| \(touch "(.*\.jobfailed)"')
jobs = []
failFiles = []
for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'Args']):
    jobs.append('{}.{}'.format(job['ClusterId'], job['ProcId']))
    with open(job['Args']) as f:
        s = f.read()
        finds = failFileRegex.findall(s)
        for i in finds:
            failFiles.append(i)

print(jobs)
with schedd.transaction() as txn:
    schedd.act(htcondor.JobAction.Hold, jobs)

for i in failFiles:
    touch(i)

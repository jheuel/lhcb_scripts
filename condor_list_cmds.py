#!/usr/bin/env python
# -*- coding: utf-8 -*-

import htcondor
import re
from os import utime


def touch(fname, times=None):
    with open(fname, 'a'):
        utime(fname, times)


schedd = htcondor.Schedd()

for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'Args']):
    print(job)

    # try:
        # with open(job['Args']) as f:
            # s = f.read()
            # finds = failFileRegex.findall(s)
            # for i in finds:
                # failFiles.append(i)
        # jobs.append('{}.{}'.format(job['ClusterId'], job['ProcId']))
    # except IOError as e:
        # print(e)
        # print('Skip job')

# print(jobs)
# with schedd.transaction() as txn:
    # schedd.act(htcondor.JobAction.Hold, jobs)

# for i in failFiles:
    # touch(i)

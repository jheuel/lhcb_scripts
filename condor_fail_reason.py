#!/usr/bin/env python
# -*- coding: utf-8 -*-

import htcondor
import re
from os import utime


schedd = htcondor.Schedd()

for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'HoldReasonSubCode', 'HoldReasonCode', 'HoldReason']):
    print(job)

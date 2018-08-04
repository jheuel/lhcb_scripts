#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('evt_types', nargs='+')
args = parser.parse_args()

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Core.DISET.RPCClient import RPCClient
import json

Script.parseCommandLine( ignoreErrors = True )

class ProductionClient():
    """ Simple helper emulating old Production Client
    """

    def __init__(self):
        """Instantiates the Workflow object and some default parameters.
        """
        self.transClient = TransformationClient()

    def getParameters(self, prodID, pname=''):
        """Get a production parameter or all of them if no parameter name specified.
        """

        result = self.transClient.getTransformation(int(prodID), True)
        if not result['OK']:
            gLogger.error(result)
            return S_ERROR('Could not retrieve parameters for production %s' % prodID)

        if not result['Value']:
            gLogger.info(result)
            return S_ERROR('No additional parameters available for production %s' % prodID)

        if pname:
            if result['Value'].has_key(pname):
                return S_OK(result['Value'][pname])
            else:
                gLogger.verbose(result)
                return S_ERROR('Production %s does not have parameter %s' % (prodID, pname))

        return result


def query_by_evt_type(eventType):
    bk = RPCClient('Bookkeeping/BookkeepingManager')
    # get productions for given event type
    res = bk.getProductionSummaryFromView(
        {'EventType': eventType, 'Visible': True})
    if not res['OK']:
        print("i am here")
        print res['Message']
        DIRAC.exit(1)
    prods = res['Value']

    # get production-IDs
    bkClient = BookkeepingClient()
    prodIDs = [prod['Production'] for prod in prods]
    prodIDs.sort()

    pr = RPCClient('ProductionManagement/ProductionRequest')
    prClient = ProductionClient()

    # loop over all productions
    rv = list()
    for prodID in prodIDs:

        res = bkClient.getProductionInformations(prodID)
        if not res['OK']:
            continue

        val = res['Value']
        info = val['Production informations']
        if not info:
            continue

        steps = val['Steps']
        if isinstance(steps, str):
            continue

        dddb = None
        conddb = None

        for step in reversed(steps):
            if step[4] and 'frompreviousstep' != step[4].lower() and not dddb:
                dddb = step[4]
            if step[5] and 'frompreviousstep' != step[5].lower() and not conddb:
                conddb = step[5]

        if not (dddb and conddb):

            # print "# Can't get tags (%s/%s) from BookeepingManager, try with ProductionRequest" \
            #       % ( dddb , conddb )
            ##
            if not dddb:
                res = prClient.getParameters(prodID, 'DDDBTag')
                if res['OK']:
                    dddb = res['Value']
            ##
            if not conddb:
                res = prClient.getParameters(prodID, 'CondDBTag')
                if res['OK']:
                    conddb = res['Value']

            # print '# got ', dddb, conddb

        if not (dddb and conddb):
            res = prClient.getParameters(prodID, 'BKInputQuery')
            simProdID = None
            if res['OK']:
                simProdID = eval(res['Value']).get('ProductionID', 0)
            else:
                res = prClient.getParameters(prodID, 'RequestID')
                if res['OK']:
                    res = pr.getProductionList(int(res['Value']))
                    if res['OK']:
                        simProdID = res['Value'][0]

            if simProdID and not dddb:
                res = prClient.getParameters(simProdID, 'DDDBTag')
                if res['OK']:
                    dddb = res['Value']
            if simProdID and not conddb:
                res = prClient.getParameters(simProdID, 'CondDBTag')
                if res['OK']:
                    conddb = res['Value']

            if simProdID and not (dddb and conddb):
                res = prClient.getParameters(simProdID, 'BKProcessingPass')
                if res['OK']:
                    step0 = eval(res['Value'])['Step0']
                    if not dddb:
                        dddb = step0['DDDb']
                    if not conddb:
                        conddb = step0['CondDb']

        files = val["Number of files"]
        events = val["Number of events"]
        path = val["Path"]

        evts = 0
        ftype = None
        for i in events:
            if i[0] in ['GAUSSHIST', 'LOG', 'SIM', 'DIGI']:
                continue
            evts += i[1]
            if not ftype:
                ftype = i[0]

        nfiles = 0
        for f in files:
            if f[1] in ['GAUSSHIST', 'LOG', 'SIM', 'DIGI']:
                continue
            if f[1] != ftype:
                continue
            nfiles += f[0]

        p0, n, p1 = path.partition('\n')
        if n:
            path = p1

        rv.append({
            'path': path,
            'dddb': dddb,
            'conddb': conddb,
            'bfiles': nfiles,
            'evts': evts,
            'prodID': prodID
        })
        print(path, dddb, conddb, nfiles, evts, prodID)
    return rv


evt_types = args.evt_types

job_dicts = list()

for e in evt_types:
    print(e)
    infos = query_by_evt_type(e)
    with open("infos_dict.json", "w") as f:
        json.dump(infos, f, indent=4, sort_keys=True)
    for info in infos:
        path_split = info["path"].split("/")
        year = path_split[2]
        sim = path_split[4]
        mag = "MagDown" if "MagDown" in info["path"] else "MagUp"
        name = "MC_{e}_{y}_{s}_{m}".format(
            e=e,
            y=year,
            s=sim,
            m=mag
        )
        job_dicts.append({
            "name": name,
            "inputdata": info["path"],
            "isSimulation": "True",
            "year": year,
            "DDDB": info["dddb"],
            "CondDB": info["conddb"]
        })

with open("job_dict.json", "w") as f:
    json.dump(job_dicts, f, indent=4, sort_keys=True)

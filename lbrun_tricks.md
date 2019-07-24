pip intsall LbEnv

lb-run --siteroot=/cvmfs/lhcb.cern.ch Urania/<version> <command>

lb-run --siteroot=/cvmfs/lhcb.cern.ch/lib --container=singularity  -c x86_64-centos7-gcc8-opt  Urania/v8r0  bash --norc

lb-sdb-query listPlatforms DaVinci v39r1p1

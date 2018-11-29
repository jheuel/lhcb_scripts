#!/usr/bin/env python
# encoding: utf-8

import argparse
import ROOT
from os.path import join


def find_trees(f, path=''):
    trees = []

    keys = None
    if path == '':
        keys = f.GetListOfKeys()
    else:
        keys = f.Get(path).GetListOfKeys()

    for j in range(keys.GetEntries()):
        k = keys.At(j)
        objPath = join(path, k.GetTitle())
        if 'TDirectoryFile' == str(k.GetClassName()):
            trees += find_trees(f, objPath)
        elif 'TTree' == str(k.GetClassName()):
            print('{}:\t{}'.format(objPath, k.GetClassName()))
            trees.append(objPath)
        else:
            print('{}:\t{}'.format(objPath, k.GetClassName()))
    return trees


parser = argparse.ArgumentParser(description='')
parser.add_argument(dest='filename', help='set file name')
parser.add_argument('-t', dest='treename', help='set tree name')
args = parser.parse_args()

f = ROOT.TFile(args.filename)
trees = []

if args.treename:
    trees.append(args.treename)
else:
    trees = find_trees(f)

for tree in trees:
    t = f.Get(tree)
    for i in t.GetListOfBranches():
        print('{}:\t{}'.format(tree, i.GetTitle()))

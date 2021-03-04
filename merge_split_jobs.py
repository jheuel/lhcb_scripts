import argparse
from glob import glob
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('files', nargs='+')
args = parser.parse_args()


for f in args.files:
    end = f.split('_')[-1]
    j, s = end.split('.')
    j = int(j)
    s = int(s)

    to = glob(f'{j}_*')[0]
    cmd = ['mv', f'{f}', f'{to}/{s:04d}']
    print(cmd)
    subprocess.run(cmd)

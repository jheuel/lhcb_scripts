#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#   lb-run LHCbDIRAC/prod download_lfns.py *.lfns ./somedir

import subprocess32 as subprocess
from os import makedirs
from os.path import join, isdir, isfile, basename
from Queue import Queue
import threading
from time import sleep
from warnings import warn
from tqdm import tqdm
import argparse


class Job(object):
    def __init__(self, lfnfile, downloadpath):
        with open(lfnfile, "r+") as f:
            self.lfns = f.read().split('\n')

        self.lfnfile = lfnfile
        self.tuplename = self.lfns[0].split("/")[-1]
        print(self.tuplename)

        self.failed = False
        self.nJobs = len(self.lfns)

        self.output_path = '{}/{}'.format(
            downloadpath,
            basename(lfnfile).split('.')[0])

        self.q = Queue()
        for ctr, path in enumerate(self.lfns):
            if path == "":
                print("job {0} is not yet finished, skipping download...".format(ctr))
                continue

            filepath = join(
                self.output_path,
                '{:04d}'.format(ctr))

            if not isdir(filepath):
                makedirs(filepath)

            if isfile(join(filepath, self.tuplename)):
                print("file {:04d} exists already".format(ctr))
                continue

            self.q.put({
                'command': 'dirac-dms-get-file {} -D {}'.format(path, filepath),
                'retries': 3,
            })


class LFNDownloader(object):
    def __init__(self, lfnfiles, downloadpath, nThreads):
        self.running = True
        self.nThreads = nThreads
        self.timeout = 15 * 60 # seconds
        self.jobs = [Job(f, downloadpath) for f in lfnfiles]

    def worker(self, pbar, input_q, output_q):
        while self.running and not input_q.empty():
            cmd = input_q.get()
            if cmd is None:
                return

            output = ''
            try:
                output = subprocess.check_output(
                        cmd['command'],
                        timeout=self.timeout,
                        stderr=subprocess.STDOUT,
                        shell=True).decode('utf-8')
            except subprocess.CalledProcessError as e:
                output = '{}\n{}\n\nfailed'.format(e, output)
            except subprocess.TimeoutExpired as e:
                output = '{}\n{}\n\nfailed (timeout)'.format(e, output)

            output_q.put(output)

            if 'no accessible replicas found' in output.lower():
                output_q.put('download failed')
                output_q.put(self.JobFailed)
            elif 'failed' in output.lower():
                cmd['retries'] -= 1
                if cmd['retries'] > 0:
                    output_q.put('add file to queue again ({} retries left)'.format(
                        cmd['retries']))
                    input_q.put(cmd)
                else:
                    pbar.update()
                    output_q.put('download failed')
                    output_q.put(self.JobFailed)
            else:
                pbar.update()
            input_q.task_done()

    QueueCloser = object()
    JobFailed = object()

    def monitor(self, logFile, output_q, job, pbar):
        with open(logFile, 'w') as f:
            while True:
                o = output_q.get()
                if o is self.JobFailed:
                    job.failed = True
                    output_q.task_done()
                    continue
                if o is self.QueueCloser:
                    output_q.task_done()
                    return
                pbar.write(o)

                f.write(o + '\n')
                output_q.task_done()

    def run(self):
        self.running = True

        tqdm_config = {
            'dynamic_ncols': True,
            'ascii': False,
        }

        for job in tqdm(self.jobs, disable=len(self.jobs)<2, **tqdm_config):
            if not self.running:
                break

            with tqdm(total=job.nJobs, **tqdm_config) as pbar:
                pbar.write('Downloading files for: {}'.format(job.tuplename))
                output_q = Queue()

                threads = [
                    threading.Thread(
                        target=self.worker,
                        args=(
                            pbar,
                            job.q,
                            output_q,
                        )
                    )
                    for i in range(self.nThreads)]

                logfile = 'log_{}.txt'.format(
                    basename(job.lfnfile).split('.')[0])

                threads.append(
                    threading.Thread(
                        target=self.monitor,
                        args=(logfile, output_q, job, pbar)
                    )
                )

                for thread in threads:
                    thread.start()

                try:
                    job.q.join()

                    if job.failed:
                        output_q.put('failed to download all files')
                    else:
                        output_q.put('{} finished'.format(job.lfnfile))

                    output_q.put(self.QueueCloser)
                    output_q.join()
                except KeyboardInterrupt:
                    self.running = False

        if any([job.failed for job in self.jobs]):
            print('\n\n\nFailed to download all files from:')
        for job in self.jobs:
            if job.failed:
                print('\t{}'.format(job.lfnfile))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('lfnfile', nargs='+')
    parser.add_argument('downloadpath', help='set downloadpath')
    parser.add_argument('-j', '--nthreads', default=10, help='set number of threads')
    args = parser.parse_args()

    dl = LFNDownloader(args.lfnfile, args.downloadpath, args.nthreads)
    dl.run()

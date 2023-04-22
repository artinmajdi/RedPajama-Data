import glob, os
import json
import sys
import re
import hashlib
import gzip
import os

from multiprocessing import Pool

os.chdir(sys.argv[1])
jobs = [
    file
    for file in glob.glob("*/*.gz")
    if ("middle" in file or "head" in file) and "dedup" not in file
]
print("TOTAL # JOBS:", len(jobs))


# Output (URL, digest) pairs for each job
#
def run(job):
    print(job)
    ofile = gzip.open(f"{job}.dedup", "wt")
    for jstr in gzip.open(job, "rt"):
        result = json.loads(jstr)
        ofile.write(result['url'] + " " + result['digest'] + "\n")
    ofile.close()

with Pool(64) as p:
    p.map(run, jobs)

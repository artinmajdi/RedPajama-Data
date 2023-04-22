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

# For each row, run classifier and output
#    (text: [...], source, pred_label, pred_label_prob, wiki_prob)
#
def run(job):

    import fasttext
    model = fasttext.load_model("../fastText/model.bin")

    print(job)
    ofile = gzip.open(f"{job}.dedup.classifier.gz", "wt")
    with open(f"{job}.dedup.classifier.gz.stat", "wt") as ostat:
        line = 0
        for jstr in gzip.open(f"{job}.result", "rt"):
            result = json.loads(jstr)
            content = result["raw_content"]
            # run classifier
            text = " ".join(content.strip().splitlines())
            pred = model.predict(text)
            (pred_label, pred_prob) = pred
            pred_label = pred_label[0]

            wiki_prob = pred_prob[0]
            if pred_label == "__label__cc":
                wiki_prob = 1 - wiki_prob

            output = {
                "pred_label": pred_label,
                "pred_label_prob": pred_prob[0],
                "wiki_prob": wiki_prob,
                "text": content,
                "source": f"cc/{job}" + f"/line{line}",
            }
            line = line + 1

            nchars = len(content)
            ostat.write(f"{nchars}\t{wiki_prob}\n")
            ofile.write(json.dumps(output) + "\n")

        ofile.close()

with Pool(224) as p:
    p.map(run, jobs)
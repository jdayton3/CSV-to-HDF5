#!/usr/bin/env python

"""Query data that has been loaded into our hdf5 file."""

import json
import h5py
import time
import StringIO
import numpy as np
import sys

# TODO:
    # try to convert code to golang :(
    # other types of files (csv)
    # gzip/zip option

# Benchmark: 25,000 samples & 1,000 genes -> 60 second to write to disk

json_query = """
    {
        "meta": {
            "CL_Name ": [
                "A375",
                "BT20",
                "HA1E",
                "MCF7"
            ],
            "SM_Pert_Type": [
                "trt_poscon"
            ],
            "SM_Dose": [
                "0.04",
                "0.12",
                "0.37",
                "1.11"
            ]
        },
        "genes": [
            "204131_s_at",
            "202388_at",
            "204418_x_at",
            "205607_s_at",
            "200059_s_at",
            "213417_at",
            "205963_s_at",
            "205067_at",
            "213702_x_at",
            "200814_at"
        ]
    }
"""
json_query = """
    {
        "meta": {
            "CL_Name ": [
                "A375",
                "BT20",
                "HA1E",
                "MCF7"
            ]
        },
        "genes": [
            "204131_s_at",
            "202388_at",
            "204418_x_at",
            "205607_s_at",
            "200059_s_at",
            "213417_at",
            "205963_s_at",
            "205067_at",
            "213702_x_at",
            "200814_at"
        ]
    }
"""

if __name__ == "__main__":
    # Start a timer.
    x = time.time()

    # Load the JSON query into a Python object and get meta/genes.
    query = json.loads(json_query)
    meta = query['meta']
    genes = sorted(query['genes'])

    # Open the HDF5.
    hdf = h5py.File("files.h5", "r")['big_file.csv']

    # Iterate through the meta query and construct the set of samples.
    for key, val in meta.items():
        s = set()
        for v in val:
            s = set.union(set(hdf["meta"][key][v].value), s)
        try:
            t
        except:
            t = s.copy()
        else:
            t = set.intersection(s, t)
        print "querying", key, "...number of samples:", len(t)

    # Get the data column indices for the genes.
    gene_ixs = []
    for g in genes:
        gene_ixs.append(hdf['genes'][g].value)

    # Print the time elapsed, so far.
    print time.time() - x, "seconds to get samples."
    
    # Get the first element in the set.
    for e in t:
        break

    # Start a timer
    y = time.time()
    
    # Start querying the hdf5 and writing to the output file.
    sys.argv.append("test.tsv")
    out_path = sys.argv[1]
    out_file = open(out_path, 'w')

    # Build and write the header for the csv.
    header = list(hdf["header"].value) + genes
    out_file.write("\t".join(header) + "\n")

    # Write the lines out
    print "Gene indices: ", gene_ixs
    for e in t:
        if len(gene_ixs) == 0:
            g = hdf["data"][e, :]
        else:
            g = hdf["data"][e, :][np.array(gene_ixs)]
        g = [str(x) for x in g]
        out_file.write("\t".join(list(hdf["samples"][e]) + list(g)) + "\n")

    out_file.close()
    print time.time() - y, "seconds to write to file."
    # We can apply a bitmask to an np array (which is what we'll be getting
    # from the hdf['data'] thing...)

    # from numpy import array
    # a = array([1,2,3,4,5,6,7,8])
    # i = [2,3,5,6]
    # a[i] # returns [3,4,6,7]
# Copyright (C) 2016 by The HDF Group.
#  All rights reserved.
#
# This PySpark example parallizes handling multiple HDF4 files.
#
# Put a bunch of AIRS.YYY.MM.DD.L3.RetStd_H031.v4.0.21.0.G06104133732.hdf files
# like [1] in the current working directory.
#
# Then, run this script like:
#
# %python summary_hdf.py .
#
# At the end, it will create summary.csv file that computes mean, median, stdev.
#
# This is tested with PySpark-Notebook Docker [2] on CentOS 7.
#
# Last updated: 2016-09-08
#
# [1] ftp://ftp.hdfgroup.uiuc.edu/pub/outgoing/NASAHDF/AIRS.2002.08.01.L3.RetStd_H031.v4.0.21.0.G06104133732.hdf
# [2] https://github.com/jupyter/docker-stacks/tree/master/pyspark-notebook
import os, sys

import pyhdf, numpy as np
from pyhdf.SD import SD, SDC

from pyspark import SparkContext

if __name__ == "__main__":
    """
    Usage: doit [partitions]
    """
    sc = SparkContext(appName="AIRS")
    base_dir = str(sys.argv[1]) if len(sys.argv) > 1 else exit(-1)
    partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    hdf4_path = 'Temperature_MW_A'

    ###################################################################
    ### get the sample count, mean, median, and standard deviation

    def summarize(x):
        a = x.split(",")
        FILE_NAME = a[0]
        DATAFIELD_NAME = a[1]

        hdf = SD(FILE_NAME, SDC.READ)
        data3D = hdf.select(DATAFIELD_NAME)
        
        # Subset at level = 11.
        data = data3D[11,:,:]
        
        # Handle fill value.
        attrs = data3D.attributes(full=1)
        fillvalue=attrs["_FillValue"]
        fv = fillvalue[0]
        v = data[data != fv]
        
        # Extract the date from the file name.
        # AIRS.2002.08.01.L3.RetStd_H031.v4.0.21.0.G06104133732.hdf
        key = "".join(FILE_NAME[6:17].split("."))
        return [(key, (len(v), np.mean(v), np.median(v), np.std(v)))]
    ###################################################################

    # traverse the base directory and pick up HDFEOS files
    file_list = filter(lambda s: s.endswith(".hdf"),
                       [ "%s%s%s" % (root, os.sep, file)
                         for root, dirs, files in os.walk(base_dir)
                         for file in files])

    # partition the list
    file_paths = sc.parallelize(
       map(lambda s: "%s,%s"%(s, hdf4_path), file_list), partitions)

    # compute per file summaries
    rdd = file_paths.flatMap(summarize)

    # collect the results and write the time series to a CSV file
    results = rdd.collect()

    with open("summary.csv", "w") as text_file:
        text_file.write("Day,Samples,Mean,Median,Stdev\n")
        for k in sorted(results):
            text_file.write(
                "{0},{1},{2},{3},{4}\n".format(
                    k[0], k[1][0], k[1][1], k[1][2], k[1][3]))

    sc.stop()

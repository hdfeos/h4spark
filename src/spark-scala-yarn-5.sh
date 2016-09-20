#!/bin/bash
java -XshowSettings:properties -version
###load single large hdf5 file####
repartition="1"
inputfile="/tmp/test.h5"
app_name="H5Sspark-udf"
dataset="/HDFEOS/GRIDS/NCEP/Data Fields/SST"
# SPARKURL="spark://localhost:7077"
# SPARKURL="local[1]"
SPARKURL="yarn"
SCRATCH="/tmp"
spark-submit --verbose\
             --master $SPARKURL\
             --deploy-mode cluster \
             --name $app_name \
             --driver-memory 4G\
             --executor-cores 1 \
             --driver-cores 1  \
             --num-executors=1 \
             --executor-memory 4G\
             --class org.nersc.io.readtest\
             --conf spark.eventLog.enabled=true\
             --conf spark.eventLog.dir=$SCRATCH/spark\
             --driver-library-path=/Users/hyoklee/Library/Java/Extensions\
             target/scala-2.11/h5spark-assembly-1.0.jar \
             $repartition "$inputfile" "$dataset"
rm /tmp/spark/*
# stop-all.sh
#stop-collectl.sh

#!/bin/bash
java -XshowSettings:properties -version
###load single large hdf5 file####
repartition="2"
# inputfile="/Users/hyoklee/h5spark/test.h5"
# inputfile="/scr/GSSTF_NCEP.3/1987/GSSTF_NCEP.3.1987.07.01.he5"
inputfile="/tmp/test.h5"
app_name="H5Sspark-udf"
dataset="/HDFEOS/GRIDS/NCEP/Data Fields/SST"
# SPARKURL="spark://idap:7077"
SPARKURL="local[8]"
SCRATCH="/tmp/"
spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 1g\
  --executor-cores 1 \
  --driver-cores 1  \
  --num-executors=4 \
  --executor-memory 1g\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark\
  target/scala-2.11/h5spark-assembly-1.0.jar \
  $repartition "$inputfile" "$dataset"


rm /tmp/spark/*
# stop-all.sh
#stop-collectl.sh

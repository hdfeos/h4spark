#!/bin/bash
java -XshowSettings:properties -version
###load single large hdf5 file####
repartition="200"
# inputfile="/Users/hyoklee/h5spark/test.h5"
#inputfile="/scr/data/NASA/HDF4/HDF-EOS2/AQUA/AIRS/Grid/AIRS.2002.08.24.L3.RetStd_H008.v4.0.21.0.G06104133343.hdf"
# inputfile="hdfs://jaguar:9000/AIRS.hdf"
app_name="H5Sspark-udf"
dataset="2"
# SPARKURL="spark://localhost:7077"
# SPARKURL="local[1]"
SPARKURL="yarn"
SCRATCH="/tmp"
spark-submit --verbose\
             --master $SPARKURL\
             --deploy-mode cluster \
  --name $app_name \
  --driver-memory 6G\
  --executor-cores 8 \
  --driver-cores 8  \
  --num-executors=5 \
  --executor-memory 6G\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark\
  target/scala-2.11/h5spark-assembly-1.0.jar \
  $repartition "$inputfile" "$dataset"


rm /scr/data/spark_tmp_dir/*
# stop-all.sh
#stop-collectl.sh

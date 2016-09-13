/************************************************************
HDF4 reader in spark
************************************************************/

package org.nersc.io

import hdf.hdflib.HDFLibrary._;
import hdf.hdflib.HDFException;
import hdf.hdflib.HDFChunkInfo;
import hdf.hdflib.HDFCompInfo;
import hdf.hdflib.HDFConstants._;


import org.slf4j.LoggerFactory
import java.io.File

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext
import scala.collection.immutable.NumericRange
object read {
  private def getdimentions(file: String, variable: String): (Array[Int],Int) = {
    println("getdimensions()")
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    try { 
      println("Calling SDopen")
      file_id = SDstart(file, DFACC_READ);
      println(file_id)
      println("Calling SDselect "+variable)      
      dataset_id = SDselect(file_id, 0)
      println(dataset_id)
    }
    catch {
      case e: Exception => logger.info("\nFile error: " + file)
    }

    var name = new Array[String](1)
    name(0) = "test"
    var dim_sizes = new Array[Int](2)
    var args = new Array[Int](3)
    dim_sizes = Array(1,1)

    var ranks = 2
    val stat = SDgetinfo(dataset_id, name, dim_sizes, args)
    println("SDgetinfo() returned:stat="+stat)
    println(name.deep.mkString("\n"))
    println(dim_sizes(0))    
    SDendaccess(dataset_id)
    SDend(file_id)
    (dim_sizes, ranks)
  }

  private def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }
  //used in reading a folder of hdf5 file, the read_whole_dataset is a serial read. 
  private def read_whole_dataset(FILENAME: String, DATASETNAME: String): (Array[Array[Double]]) = {
    println("read_whole_dataset()")
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    var dset_dims = new Array[Long](2)
    dset_dims = Array(1, 1)
    var dim_sizes = new Array[Int](2)
    dim_sizes = Array(1,1)    
    var args = new Array[Int](3)
    var name = new Array[String](1)
    name(0) = "test"
    var start = new Array[Int](2)
    start = Array(0,0)
    var edges = new Array[Int](2)
    var strides = new Array[Int](2)
    strides = Array(1,1)    
    var dread_id = false
    
    //Open file
    try {
      file_id = SDstart(FILENAME, 0)
    }
    catch {
      case e: Exception => logger.info("\nFile open error,filename:" + FILENAME)
    }
    //Open dataset/variable
    try {
      dataset_id = SDselect(file_id, 0)
    }
    catch {
      case e: Exception => logger.info("\nDataset open error:" + DATASETNAME)
    }
    //Get dimension info
    try {
      val stat = SDgetinfo(dataset_id, name, dim_sizes, args)    
    }
    catch {
      case e: Exception => logger.info("Dataspace open error,dataspace_id: " + dataspace_id)
    }
    if (dset_dims(0) > 0 && dset_dims(1) > 0) {
      val dset_data = Array.ofDim[Double](dset_dims(0).toInt, dset_dims(1).toInt)
      try {
        edges(0) = dim_sizes(0)
        edges(1) = dim_sizes(1)
        dread_id = SDreaddata(dataset_id, start, strides, edges, dset_data)
      }
      catch {
        case e: java.lang.NullPointerException => logger.info("data object is null")
        case e: java.lang.NegativeArraySizeException => logger.info("emptyjavaarray" + e.getMessage + e.printStackTrace + FILENAME)
      }
      if (dread_id == false)
        logger.info("Dataset open error" + FILENAME)
      dset_data
    }
    else {
      val dset_data = Array.ofDim[Double](1, 1)
      logger.info("file empty" + FILENAME)
      dset_data
    }
  }

  private def read_hyperslab(FILENAME: String, DATASETNAME: String, start: Long, end: Long): (Array[Double], Array[Long]) = {
    println("read_hyperslab()")
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    var dim_sizes = new Array[Int](2)
    dim_sizes = Array(1,1)    
    var args = new Array[Int](3)    
    var name = new Array[String](1)
    name(0) = "test"    
    var starts = new Array[Int](2)
    var edges = new Array[Int](2)
    var strides = new Array[Int](2)    
    //var ranks: Int = 2
    //var dset_dims = new Array[Long](2)
    var end1 = end
    /*Open an existing file*/
    try {
      file_id = SDstart(FILENAME, DFACC_READ);
    }
    catch {
      case e: Exception => logger.info("\nFile error: " + FILENAME)
    }
    /*Open an existing dataset/variable*/
    try {
      dataset_id = SDselect(file_id, 0)
    }
    catch {
      case e: Exception => logger.info("\nDataset error\n")
    }
    //Get dimension information of the dataset
    try {
        val stat = SDgetinfo(dataset_id, name, dim_sizes, args)    
    }
    catch {
      case e: Exception => logger.info("\nDataspace error")
    }
    
    var (dset_dims:Array[Int],ranks:Int) = getdimentions(FILENAME, DATASETNAME)
    //Adjust last access
    if (end1 > dset_dims(0))
      end1 = dset_dims(0)
    val step = end1 - start
    var subset_length: Long = 1
    logger.info("Ranks="+ranks)
    logger.info("Dim 0="+dset_dims(0))
    for (i <- 1 to ranks-1) {
      subset_length *= dset_dims(i)
      logger.info("Dim "+i+"="+dset_dims(i))
    }

    val dset_datas = Array.ofDim[Double](step.toInt * subset_length.toInt)

    val start_dims: Array[Long] = new Array[Long](ranks)
    val count_dims: Array[Long] = new Array[Long](ranks)
    start_dims(0) = start.toLong
    count_dims(0) = step.toLong
    for (i <- 1 to ranks-1) {
      start_dims(i) = 0.toLong
      count_dims(i) = dset_dims(i)
    }
    var hyper_id = -2
    var dread_id = false
    var memspace = -2
    /*read data*/
    try {
      val dset_data = Array.ofDim[Double](dset_dims(0).toInt, dset_dims(1).toInt)    
      dread_id = SDreaddata(dataset_id, starts, strides, edges, dset_datas)
    }
    catch {
      case e: java.lang.NullPointerException => logger.info("data object is null")

    }
    var global_start = (start - 1) * subset_length
    if (global_start < 0) global_start = 0
    var global_end = end1 * subset_length
    import Array._
    val index: Array[Long] = NumericRange(global_start, global_end, 1).toArray
    SDendaccess(dataset_id)
    SDend(file_id)    
    (dset_datas, index)
  }

  private def read_array(FILENAME: String, DATASETNAME: String, start: Long, end: Long): (Array[Array[Double]]) = {
    var end1 = end
    var (dset_dims:Array[Int], ranks:Int) = getdimentions(FILENAME, DATASETNAME)
    //Adjust last access
    if (end1 > dset_dims(0))
      end1 = dset_dims(0)
    var subset_length: Long = 1
    for (i <- 1 to ranks-1) {
      subset_length *= dset_dims(i)
    }
    var dset_data: Array[Array[Double]] = Array.ofDim((end1 - start).toInt, subset_length.toInt)
    var (dset_datas: Array[Double], index: Array[Long]) = read_hyperslab(FILENAME, DATASETNAME, start, end)
    for (id <- 0 to (end1 - start).toInt - 1) {
      for (jd <- 0 to subset_length.toInt - 1) {
        dset_data(id)(jd) = dset_datas(id * subset_length.toInt + jd)
      }
    }
    dset_data
  }

  def h5read_point(sc:SparkContext, inpath: String, variable: String, partitions: Long): RDD[(Double,Long)] = {
    val file = new File(inpath)
    val logger = LoggerFactory.getLogger(getClass)
    if (file.exists && file.isFile) {
      //read single file
      logger.info("Read Single file:" + inpath)
      val (dims: Array[Int],ranks:Int) = getdimentions(inpath, variable)
      val rows: Long = dims(0)
      var num_partitions: Long = partitions
      if (rows < num_partitions) {
        num_partitions = rows
      }
      println("num_partitions="+num_partitions)
      val step: Long = rows / num_partitions
      val arr = sc.range(0, rows, step, partitions.toInt).flatMap(x =>{
          var data_array = read_hyperslab(inpath, variable, x, x + step)
          (data_array._1 zip data_array._2)
      })
      arr
    }
    else {
      val okext = List("hdf", "he2")
      val listf = getListOfFiles(file, okext)
      logger.info("Read" + listf.length + " files from directory:" + inpath)
      val arr = sc.parallelize(listf, partitions.toInt).map(x => x.toString).flatMap(x =>{
          var data_array = read_hyperslab(inpath, variable, 0,1e100.toLong)
          (data_array._1 zip data_array._2)
      })
      arr
    }
  }

  def h5read_array(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[Array[Double]] = {
    val file = new File(inpath)
    val logger = LoggerFactory.getLogger(getClass)
    if (file.exists && file.isFile) {
      //read single file
      logger.info("Read Single file:" + inpath)

      val (dims: Array[Int],ranks:Int) = getdimentions(inpath, variable)
      val rows: Long = dims(0)
      var num_partitions: Long = partitions
      if (rows < num_partitions) {
        num_partitions = rows
      }
      val step: Long = rows / num_partitions
      val arr = sc.range(0, rows, step, partitions.toInt).flatMap(x => read_array(inpath, variable, x, x + step))
      arr
    }
    else {
      val okext = List("hdf", "he2")
      val listf = getListOfFiles(file, okext)
      logger.info("Read" + listf.length + " files from directory:" + inpath)
      val arr = sc.parallelize(listf, partitions.toInt).map(x => x.toString).flatMap(x => read_whole_dataset(x, variable))
      arr
    }
  }

  def h5read_vec(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[DenseVector] = {
    val arr = h5read_array(sc, inpath, variable, partitions).map {
      case a: Array[Double] =>
        new DenseVector(a)
    }
    arr
  }

  def h5read_irow(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[IndexedRow] = {
    val irow = h5read_vec(sc, inpath, variable, partitions).zipWithIndex().map(k => (k._1, k._2)).map(k => new IndexedRow(k._2, k._1))
    irow
  }

  def h5read_imat(sc: SparkContext, inpath: String, variable: String, partitions: Long): IndexedRowMatrix = {
    val irow = h5read_irow(sc, inpath, variable, partitions)
    val imat = new IndexedRowMatrix(irow)
    imat
  }

}


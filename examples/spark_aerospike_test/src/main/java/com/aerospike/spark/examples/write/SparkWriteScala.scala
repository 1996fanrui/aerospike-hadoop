package com.aerospike.spark.examples.write

import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteScala {

  def main(args: Array[String]): Unit = {

    val appName = "spark_aerospike_test"
    val conf = new SparkConf()
    conf.setAppName(appName)
      .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://nameservice:8020/tmp/aerospike/users.log")
    val rdd2 = rdd.map( line=>{
      val split = line.split(' ')
      (line,new Session(split(0), split(1).toInt))
    } )
    AerospikeOutputUtil.savaAerospike(rdd2)
    sc.stop()
  }

}

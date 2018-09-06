package com.aerospike.spark.examples.write

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadParquetWriteAerospike{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("parquet")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val load = sqlContext.read.format("parquet")
          .load("hdfs://nameservice:8020/user/hive/warehouse/test.db/tmp_aerospike_data/");
    //  load = sqlContext.read().parquet("./sparksql/parquet");
    load.show()
    load.printSchema()
    val c = load.count()
    println
    println
    println
    println( "行数为" + c )
    println
    println
    println
    val rdd2 = load.map( row=>{
      (row.getAs(0).toString,new Session(row.getAs(0).toString,row.getAs(1).asInstanceOf[Integer]))
    } ).repartition(300)
    AerospikeOutputUtil.savaAerospike(rdd2)
    sc.stop()
  }
}

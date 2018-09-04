package com.aerospike.spark.examples

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadParquet{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("parquet")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val load = sqlContext.read.format("parquet")
          .load("hdfs://nameservice:8020/user/hive/warehouse/prod.db/ads_hive_data_api_item_id_stat_1d/ds=20180903");
//  load = sqlContext.read().parquet("./sparksql/parquet");
    load.show()
    println( load.count() )

    sc.stop()
  }
}

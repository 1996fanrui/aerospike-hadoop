package com.aerospike.spark.examples.write;

import com.aerospike.hadoop.mapreduce.AerospikeLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkWriteTest {

    public static final String appName = "spark_aerospike_test";

    public static class ExtractUsers
        implements PairFunction<String, String, Session> {

        // Sample line format:
        // xiaoming 20

        public Tuple2<String, Session> call(String line) {
            String[] split = line.split(" ");
            String userName = split[0];
            int userAge = Integer.parseInt(split[1]);
            Session session = new Session(userName, userAge);
            return new Tuple2<String, Session>(line, session);
        }
    }


    public static void main(String[] args) {
        com.aerospike.client.Log.setCallback(new AerospikeLogger());
        com.aerospike.client.Log.setLevel(com.aerospike.client.Log.Level.DEBUG);
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .set("spark.executor.memory", "2g");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.addJar("build/libs/spark_aerospike_test-notfull.jar");

        JavaRDD<String> entries = sc.textFile("hdfs://nameservice:8020/tmp/aerospike/users.log");

        JavaPairRDD<String, Session> users = entries.mapToPair( new ExtractUsers() );

        System.err.println(users.count());

        AerospikeOutputUtil.savaAerospike(users);
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab

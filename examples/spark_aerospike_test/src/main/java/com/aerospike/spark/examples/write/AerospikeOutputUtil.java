package com.aerospike.spark.examples.write;

import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.spark.examples.async.AsyncSessionOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.PairRDDFunctions;

public class AerospikeOutputUtil {
    private static Class outputKeyClass = String.class;
    private static Class outputValueClass = Session.class;
    private static Class outputFormatClass = SessionOutputFormat.class;
//    private static Class outputFormatClass = AsyncSessionOutputFormat.class;
//    private static String hosts = "192.168.30.216";
//    private static String hosts = "node1.aerospike.bigdata.wl.com,node2.aerospike.bigdata.wl.com,node3.aerospike.bigdata.wl.com,node4.aerospike.bigdata.wl.com";
    private static String hosts = "node1.aerospike.bigdata.wl.com";
    private static int port = 3000;
    private static String namespace = "dmp";
    private static String setName = "users2";

    private static JobConf getJob() {
        JobConf job = new JobConf();
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setOutputFormat(outputFormatClass);

        AerospikeConfigUtil.setOutputHost(job, hosts);
        AerospikeConfigUtil.setOutputPort(job, port);
        AerospikeConfigUtil.setOutputNamespace(job, namespace);
        AerospikeConfigUtil.setOutputSetName(job, setName);

        return job;
    }


    public static void savaAerospike(JavaPairRDD<String,Session> rdd) {
        rdd.saveAsHadoopDataset( getJob() );
    }

    public static void savaAerospike(PairRDDFunctions<String,Session> rdd) {
        rdd.saveAsHadoopDataset( getJob() );
    }


    public static Class getOutputKeyClass() {
        return outputKeyClass;
    }

    public static void setOutputKeyClass(Class outputKeyClass) {
        AerospikeOutputUtil.outputKeyClass = outputKeyClass;
    }

    public static Class getOutputValueClass() {
        return outputValueClass;
    }

    public static void setOutputValueClass(Class outputValueClass) {
        AerospikeOutputUtil.outputValueClass = outputValueClass;
    }

    public static Class getOutputFormatClass() {
        return outputFormatClass;
    }

    public static void setOutputFormatClass(Class outputFormatClass) {
        AerospikeOutputUtil.outputFormatClass = outputFormatClass;
    }

    public static String getHosts() {
        return hosts;
    }

    public static void setHosts(String hosts) {
        AerospikeOutputUtil.hosts = hosts;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        AerospikeOutputUtil.port = port;
    }

    public static String getNamespace() {
        return namespace;
    }

    public static void setNamespace(String namespace) {
        AerospikeOutputUtil.namespace = namespace;
    }

    public static String getSetName() {
        return setName;
    }

    public static void setSetName(String setName) {
        AerospikeOutputUtil.setName = setName;
    }
}

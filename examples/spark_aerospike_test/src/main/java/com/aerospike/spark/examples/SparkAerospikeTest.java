/* 
 * Copyright 2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more
 * contributor license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.aerospike.spark.examples;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.util.Progressable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import com.aerospike.hadoop.mapreduce.AerospikeConfigUtil;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;
import com.aerospike.hadoop.mapreduce.AerospikeLogger;

public class SparkAerospikeTest {

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

    private static class Session {
        public String userName;
        public int userAge;

        public Session(String userName, int userAge) {
            this.userName = userName;
            this.userAge = userAge;
        }
    }

    public static class SessionOutputFormat
        extends AerospikeOutputFormat<String, Session> {

        public static class SessionRecordWriter
            extends AerospikeRecordWriter<String, Session> {

            public SessionRecordWriter(Configuration cfg,
                                       Progressable progressable) {
                super(cfg, progressable);
            }

            @Override
            public void writeAerospike(String sessid,
                                       Session session,
                                       AerospikeClient client,
                                       WritePolicy writePolicy,
                                       String namespace,
                                       String setName) throws IOException {
                Key kk = new Key(namespace, setName, sessid.toString());
                Bin bin0 = new Bin("userid", session.userAge);
                Bin bin1 = new Bin("start", session.userName);
                client.put(writePolicy, kk, bin0, bin1);
            }
        }

        public RecordWriter<String, Session>
            getAerospikeRecordWriter(Configuration conf, Progressable prog) {
            return new SessionRecordWriter(conf, prog);
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

        JobConf job = new JobConf();
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Session.class);
        job.setOutputFormat(SessionOutputFormat.class);

        AerospikeConfigUtil.setOutputHost(job, "192.168.30.216");
        AerospikeConfigUtil.setOutputPort(job, 3000);
        AerospikeConfigUtil.setOutputNamespace(job, "test");
        AerospikeConfigUtil.setOutputSetName(job, "users3");

        users.saveAsHadoopDataset(job);
    }
}

// Local Variables:
// mode: java
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: nil
// End:
// vim: softtabstop=4:shiftwidth=4:expandtab

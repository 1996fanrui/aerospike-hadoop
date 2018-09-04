package com.aerospike.spark.examples.write;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.PairRDDFunctions;

import java.io.IOException;

public class SessionOutputFormat
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
            Key kk = new Key(namespace, setName, sessid);
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

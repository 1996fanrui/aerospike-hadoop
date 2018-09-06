package com.aerospike.spark.examples.async;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeClientSingleton;
import com.aerospike.hadoop.mapreduce.AerospikeOutputFormat;
import com.aerospike.hadoop.mapreduce.AerospikeRecordWriter;
import com.aerospike.spark.examples.write.Session;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;

public class AsyncSessionOutputFormat
        extends AerospikeOutputFormat<String, Session> {

    public static class SessionRecordWriter
            extends AerospikeRecordWriter<String, Session> {

        // Allocate an event loop for each cpu core
        private final int eventLoopSize = Runtime.getRuntime().availableProcessors();
        // important config to use async client api in storm, Allow concurrent commands per event loop.
        private final int concurrentMax = eventLoopSize * 20;
        private transient EventLoopGroup eventLoopGroup;
        private transient EventPolicy eventPolicy;
        private transient EventLoops eventLoops;

        public SessionRecordWriter(Configuration cfg,
                                   Progressable progressable) {
            super(cfg, progressable);
        }

        @Override
        protected void init() throws IOException {
            super.init();
            eventLoopGroup = new NioEventLoopGroup(eventLoopSize);
            eventPolicy = new EventPolicy();
            eventLoops = new NettyEventLoops(eventPolicy, eventLoopGroup);
            super.policy.maxConnsPerNode = eventLoopSize;
            super.policy.eventLoops = eventLoops;
            super.writePolicy = policy.writePolicyDefault;
            super.writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
            super.writePolicy.commitLevel = CommitLevel.COMMIT_MASTER;
//            super.writePolicy.sendKey = true;
            super.client = AerospikeClientSingleton.getInstance(policy, super.host, super.port);
        }

        @Override
        public void writeAerospike(String sessid,
                                   Session session,
                                   AerospikeClient client,
                                   WritePolicy writePolicy,
                                   String namespace,
                                   String setName) throws IOException {
            Key key = new Key(namespace, setName, sessid);
            Bin bin0 = new Bin("userid", session.userAge);
            Bin bin1 = new Bin("start", session.userName);
            client.put( eventLoops.next(), new AerospikeWriteListener(eventLoops,writePolicy,key,bin0,bin1), writePolicy, key, bin0, bin1 );
        }
    }

    public RecordWriter<String, Session>
    getAerospikeRecordWriter(Configuration conf, Progressable prog) {
        return new SessionRecordWriter(conf, prog);
    }


}

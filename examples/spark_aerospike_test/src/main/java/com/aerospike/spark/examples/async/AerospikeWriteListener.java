package com.aerospike.spark.examples.async;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.hadoop.mapreduce.AerospikeClientSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeWriteListener implements WriteListener {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeWriteListener.class);
    AerospikeClient client;
    private transient EventLoops eventLoops;
    private WritePolicy writePolicy;
    private Key key;
    private Bin[] bins;

    public AerospikeWriteListener(EventLoops eventLoops, WritePolicy writePolicy, Key key, Bin... bins) {
        this.client = client;
        this.eventLoops = eventLoops;
        this.writePolicy = writePolicy;
        this.key = key;
        this.bins = bins;
    }

    public void onSuccess(Key key) {
        LOG.error("Success===>{}",key.userKey);
    }

    public void onFailure(AerospikeException e) {
        client = AerospikeClientSingleton.getInstance(null, null, 3000);
        client.put( eventLoops.next(), new AerospikeWriteListener(eventLoops,writePolicy,key,bins), writePolicy, key, bins );
        LOG.error("Fail===>{}" + key.userKey,e.getMessage());
    }
}

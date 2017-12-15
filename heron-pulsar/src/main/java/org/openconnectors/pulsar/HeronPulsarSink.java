package org.openconnectors.pulsar;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Sink;
import org.openconnectors.PulsarSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class HeronPulsarSink implements Sink<byte[]> {
    private static final long serialVersionUID = -5141054170369556410L;
    private static final Logger LOG = LoggerFactory.getLogger(HeronPulsarSink.class);
    private PulsarSink sink;

    public HeronPulsarSink(PulsarSink sink) {
        this.sink = sink;
    }

    @Override
    public void setup(Context context) {
        try {
            sink.start();
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Kafka Sink", e);
        }
    }

    @Override
    public void put(byte[] tuple) {
        sink.publish(Collections.singleton(tuple));

    }

    @Override
    public void cleanup() {
        try {
            sink.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Pulsar Sink", e);
        }
    }
}

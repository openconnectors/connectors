package org.openconnectors.pulsar;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Sink;
import org.openconnectors.PulsarSink;
import org.openconnectors.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class HeronPulsarSink implements Sink<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(HeronPulsarSink.class);
    private PulsarSink sink;
    @Override
    public void setup(Context context) {
        if (sink == null) {
            sink = new PulsarSink();
        }
        try {
            sink.open(new ConfigProvider());
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

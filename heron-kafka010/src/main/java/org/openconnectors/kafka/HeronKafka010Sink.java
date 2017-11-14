package org.openconnectors.kafka;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Sink;
import org.openconnectors.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class HeronKafka010Sink<K, V> implements Sink<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(HeronKafka010Sink.class);
    private KafkaSink010<K, V> sink;

    @Override
    public void setup(Context ctx) {
        if (sink == null) {
            sink = new KafkaSink010<>();
        }
        try {
            sink.open(new ConfigProvider());
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Kafka Sink", e);
        }
    }

    @Override
    public void put(KeyValue<K, V> tuple) {
        sink.publish(Collections.singleton(new org.openconnectors.util.KeyValue<>(tuple.getKey(), tuple.getValue())));
    }

    @Override
    public void cleanup() {
        try {
            sink.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Kafka Sink", e);
        }
    }
}
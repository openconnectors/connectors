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
    private static final long serialVersionUID = -4449590477905912986L;
    private KafkaSink010<K, V> kafkaSink;

    public HeronKafka010Sink(KafkaSink010<K, V> kafkaSink) {
        this.kafkaSink = kafkaSink;
    }

    @Override
    public void setup(Context ctx) {
    }

    @Override
    public void put(KeyValue<K, V> tuple) {
        kafkaSink.publish(Collections.singleton(new org.openconnectors.util.KeyValue<>(tuple.getKey(), tuple.getValue())));
    }

    @Override
    public void cleanup() {
        try {
            kafkaSink.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Kafka Sink", e);
        }
    }
}
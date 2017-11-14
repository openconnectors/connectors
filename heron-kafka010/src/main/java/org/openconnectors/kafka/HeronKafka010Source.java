package org.openconnectors.kafka;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Source;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.util.PullWrapperOnPushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class HeronKafka010Source<V> implements Source<V> {

    private static final Logger LOG = LoggerFactory.getLogger(HeronKafka010Source.class);
    private PullWrapperOnPushSource<V> pullHose;

    @Override
    public void setup(Context context) {
        if (pullHose == null) {
            KafkaSource010<V> kafkaSource = new KafkaSource010<>();
            pullHose = new PullWrapperOnPushSource<> (kafkaSource, 1024 * 1024,
                PullWrapperOnPushSource.BufferStrategy.BLOCK);
        }
        try {
            pullHose.open(new ConfigProvider());
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Kafka Source", e);
        }
    }

    @Override
    public Collection<V> get() {
        try {
            return pullHose.fetch();
        } catch (Exception e) {
            LOG.error("Exception thrown while fetching from Kafka Source");
        }
        return null;
    }

    @Override
    public void cleanup() {
        try {
            pullHose.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Kafka Source");
        }
    }
}
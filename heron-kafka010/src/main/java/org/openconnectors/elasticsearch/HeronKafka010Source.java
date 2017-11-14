package org.openconnectors.elasticsearch;

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
    private Collection<V> buffer;

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
    public V get() {
        if (buffer == null || buffer.isEmpty()) {
            try {
                buffer = pullHose.fetch();
            } catch (Exception e) {
                LOG.error("Exception thrown while fetching from Kafka Source");
            }
        }
        if (buffer != null && !buffer.isEmpty()) {
            V retVal = buffer.iterator().next();
            buffer.clear();
            return retVal;
        } else {
            return null;
        }
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
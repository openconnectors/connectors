package org.openconnectors.pulsar;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Source;
import org.openconnectors.PulsarSource;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.util.PullWrapperOnPushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class HeronPulsarSource implements Source<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(HeronPulsarSource.class);
    private PullWrapperOnPushSource<byte[]> pullHose;

    @Override
    public void setup(Context context) {
        if (pullHose == null) {
            PulsarSource pulsarSource = new PulsarSource();
            pullHose = new PullWrapperOnPushSource<> (pulsarSource, 1024 * 1024,
                    PullWrapperOnPushSource.BufferStrategy.BLOCK);
        }
        try {
            pullHose.open(new ConfigProvider());
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Kafka Source", e);
        }

    }

    @Override
    public Collection<byte[]> get() {
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

package org.openconnectors.pulsar;

import org.openconnectors.PulsarSource;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

public class PulsarToStdOutCopier extends SimpleCopier<byte[], String> {
    public PulsarToStdOutCopier() {
        super(new PulsarSource(), new StdoutSink(), message -> new String(message));
    }

    public static void main(String[] args) throws Exception {
        PulsarToStdOutCopier instance = new PulsarToStdOutCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}

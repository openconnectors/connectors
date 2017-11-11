package org.openconnectors.pulsar;

import org.openconnectors.PulsarSink;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdinSource;
import org.openconnectors.util.SimpleCopier;

public class StdInToPulsarCopier extends SimpleCopier<String, byte[]> {
    public StdInToPulsarCopier() {
        super(new StdinSource(), new PulsarSink(), s -> s.getBytes());
    }

    public static void main(String[] args) throws Exception {
        StdInToPulsarCopier instance = new StdInToPulsarCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}

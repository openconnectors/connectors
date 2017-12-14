package org.openconnectors.pulsar;

import org.openconnectors.PulsarSource;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

public class PulsarToStdOutCopier extends SimpleCopier<byte[], String> {
    public PulsarToStdOutCopier() {
        PulsarSource source = PulsarSource.newBuilder()
                .setTopic("persistent://sample/standalone/ns1/stdout-copier")
                .build();

        super(source, new StdoutSink(), message -> new String(message));
    }

    public static void main(String[] args) throws Exception {
        PulsarToStdOutCopier copier = new PulsarToStdOutCopier();
        copier.setup();
        copier.run();
    }
}

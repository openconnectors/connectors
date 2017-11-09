package org.openconnectors.kafka;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

/**
 * Basic topology to copy data from a kafka topic to stdout, useful for experimentation
 */
public class KafkaToStdOutCopier extends SimpleCopier<String, String> {

    public KafkaToStdOutCopier() {
        super(new KafkaSource(), new StdoutSink(), x -> x);
    }

    public static void main(String[] args) throws Exception {
        KafkaToStdOutCopier instance = new KafkaToStdOutCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}

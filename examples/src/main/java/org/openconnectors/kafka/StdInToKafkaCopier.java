package org.openconnectors.kafka;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdinSource;
import org.openconnectors.util.KeyValue;
import org.openconnectors.util.SimpleCopier;

/**
 * Basic topology to copy data from a stdin to kafka, useful for experimentation
 */
public class StdInToKafkaCopier extends SimpleCopier<String, KeyValue<String, String>> {

    public StdInToKafkaCopier() {
        super(new StdinSource(), new KafkaSink010<>(), x -> new KeyValue<>(x, x));
    }

    public static void main(String[] args) throws Exception {
        StdInToKafkaCopier instance = new StdInToKafkaCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}
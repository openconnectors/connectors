package org.openconnectors.token;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

public class BitcoinFeedToStdOutCopier extends SimpleCopier<String, String> {

    public BitcoinFeedToStdOutCopier() {
        super(new BitcoinFeedSource(Location.ASIA), new StdoutSink(), x -> x);
    }

    public static void main(String[] args) throws Exception {
        BitcoinFeedToStdOutCopier instance = new BitcoinFeedToStdOutCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}
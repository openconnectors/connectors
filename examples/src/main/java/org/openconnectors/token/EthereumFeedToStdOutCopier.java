package org.openconnectors.token;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

public class EthereumFeedToStdOutCopier extends SimpleCopier<String, String> {

    public EthereumFeedToStdOutCopier() {
        super(new EthereumFeedSource(), new StdoutSink(), x -> x);
    }

    public static void main(String[] args) throws Exception {
        EthereumFeedToStdOutCopier instance = new EthereumFeedToStdOutCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}
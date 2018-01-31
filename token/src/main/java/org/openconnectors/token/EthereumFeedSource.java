package org.openconnectors.token;

public class EthereumFeedSource extends StreamingFeedSource {

    public EthereumFeedSource(Location location) {
        super(location);
    }

    @Override
    public CurrencyPair getCurrencyPair() {
        return CurrencyPair.ETH_USD;
    }
}

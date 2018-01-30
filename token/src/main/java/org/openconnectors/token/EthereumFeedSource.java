package org.openconnectors.token;

import org.knowm.xchange.currency.CurrencyPair;

public class EthereumFeedSource extends FeedSource {

    public EthereumFeedSource(Location location) {
        super(location);
    }

    @Override
    public CurrencyPair getCurrencyPair() {
        return CurrencyPair.ETH_USD;
    }
}



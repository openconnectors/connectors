package org.openconnectors.token;

import org.knowm.xchange.currency.CurrencyPair;

public class EthereumFeedSource extends FeedSource {

    @Override
    public CurrencyPair getCurrencyPair() {
        return CurrencyPair.ETH_USD;
    }
}



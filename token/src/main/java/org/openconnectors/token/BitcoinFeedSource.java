package org.openconnectors.token;

import org.knowm.xchange.currency.CurrencyPair;

public class BitcoinFeedSource extends FeedSource {

    public BitcoinFeedSource(Location location) {
        super(location);
    }

    @Override
    public CurrencyPair getCurrencyPair() {
        return CurrencyPair.BTC_USD;
    }
}

package org.openconnectors.token;

public class BitcoinFeedSource extends StreamingFeedSource {

    public BitcoinFeedSource(Location location) {
        super(location);
    }

    @Override
    public CurrencyPair getCurrencyPair() {
        return CurrencyPair.BTC_USD;
    }
}

package org.openconnectors.token;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.bitfinex.v1.BitfinexExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PullSourceConnector;

import java.util.Collection;
import java.util.Collections;

public abstract class FeedSource implements PullSourceConnector<String> {

    private Exchange bitFinex;
    private MarketDataService marketDataService;
    private Ticker ticker;

    @Override
    public Collection<String> fetch() {
        return Collections.singleton(ticker.getCurrencyPair().toString());
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        // do nothing
    }

    @Override
    public void open(Config config) throws Exception {
        bitFinex = ExchangeFactory.INSTANCE.createExchange(BitfinexExchange.class.getName());
        marketDataService = bitFinex.getMarketDataService();
        ticker = marketDataService.getTicker(this.getCurrencyPair());
    }

    @Override
    public void close() {
        ticker = null;
        marketDataService = null;
        bitFinex = null;
    }

    @Override
    public String getVersion() {
        return TokenConfigKeys.TOKEN_CONNECTOR_VERSION;
    }

    public abstract CurrencyPair getCurrencyPair();

}

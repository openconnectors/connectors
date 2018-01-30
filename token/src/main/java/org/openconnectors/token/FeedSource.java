package org.openconnectors.token;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PullSourceConnector;

import java.util.Collection;
import java.util.Collections;

public abstract class FeedSource implements PullSourceConnector<String> {

    private Exchange exchange;
    private MarketDataService marketDataService;
    private Ticker ticker;
    private final Location location;

    public FeedSource(Location location){
        this.location = location;
    }


    @Override
    public Collection<String> fetch() {
        return Collections.singleton(ticker.toString());
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        // do nothing
    }

    @Override
    public void open(Config config) throws Exception {
        exchange = ExchangeFactory.INSTANCE.createExchange(location.EXCHANGE_CLASS.getName());
        marketDataService = exchange.getMarketDataService();
        ticker = marketDataService.getTicker(this.getCurrencyPair());
    }

    @Override
    public void close() {
        ticker = null;
        marketDataService = null;
        exchange = null;
    }

    @Override
    public String getVersion() {
        return TokenConfigKeys.TOKEN_CONNECTOR_VERSION;
    }

    public abstract CurrencyPair getCurrencyPair();

}

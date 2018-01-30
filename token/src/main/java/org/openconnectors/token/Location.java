package org.openconnectors.token;

import org.knowm.xchange.BaseExchange;
import org.knowm.xchange.bitfinex.v1.BitfinexExchange;
import org.knowm.xchange.ccex.CCEXExchange;
import org.knowm.xchange.gemini.v1.GeminiExchange;

public enum Location {
    ASIA(BitfinexExchange.class),
    EUROPE(CCEXExchange.class),
    NORTH_AMERICA(GeminiExchange.class)
    ;

    public final Class<? extends BaseExchange> EXCHANGE_CLASS;

    Location(final Class<? extends BaseExchange> exchangeClass){
        this.EXCHANGE_CLASS = exchangeClass;
    }

}

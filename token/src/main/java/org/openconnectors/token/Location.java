package org.openconnectors.token;

public enum Location {
    ASIA("Bitfinex"),
    EUROPE("Gemini"),
    NORTH_AMERICA("Poloniex")
    ;

    public final String EXCHANGE_NAME;

    Location(final String exchangeName){
        this.EXCHANGE_NAME = exchangeName;
    }

}

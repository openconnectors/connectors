package org.openconnectors.token;

public enum CurrencyPair {

    BTC_USD("BTC", "USD"),
    ETH_USD("ETH", "USD");

    private String sourceCurCode;
    private String targetCurCode;

    CurrencyPair(String sourceCurCode, String targetCurCode){
        this.sourceCurCode = sourceCurCode;
        this.targetCurCode = targetCurCode;
    }

    public String getSourceCurCode(){
        return sourceCurCode;
    }

    public String getTargetCurCode(){
        return targetCurCode;
    }
}

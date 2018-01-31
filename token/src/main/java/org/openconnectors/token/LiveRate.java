package org.openconnectors.token;

public class LiveRate {

    private String exchangeName;
    private String fromCurrency;
    private String toCurrency;
    private byte flag;
    private double price;
    private long lastUpdate;
    private double volume24h;
    private double volume24hTo;

    public LiveRate(String exchangeName, String fromCurrency, String toCurrency, byte flag, double price,
                    long lastUpdate, double volume24h, double volume24hTo) {
        super();
        this.exchangeName = exchangeName;
        this.fromCurrency = fromCurrency;
        this.toCurrency = toCurrency;
        this.flag = flag;
        this.price = price;
        this.lastUpdate = lastUpdate;
        this.volume24h = volume24h;
        this.volume24hTo = volume24hTo;
    }

    public LiveRate() {
        super();
    }

    public LiveRate(double price) {
        super();
        this.price = price;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getFromCurrency() {
        return fromCurrency;
    }

    public void setFromCurrency(String fromCurrency) {
        this.fromCurrency = fromCurrency;
    }

    public String getToCurrency() {
        return toCurrency;
    }

    public void setToCurrency(String toCurrency) {
        this.toCurrency = toCurrency;
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public double getVolume24h() {
        return volume24h;
    }

    public void setVolume24h(double volume24h) {
        this.volume24h = volume24h;
    }

    public double getVolume24hTo() {
        return volume24hTo;
    }

    public void setVolume24hTo(double volume24hTo) {
        this.volume24hTo = volume24hTo;
    }

    @Override
    public String toString() {
        return "LiveRate [exchangeName=" + exchangeName + ", fromCurrency=" + fromCurrency + ", toCurrency="
            + toCurrency + ", flag=" + flag + ", price=" + price + ", lastUpdate=" + lastUpdate + ", volume24h="
            + volume24h + ", volume24hTo=" + volume24hTo + "]";
    }

}
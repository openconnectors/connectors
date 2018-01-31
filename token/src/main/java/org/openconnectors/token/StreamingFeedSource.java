package org.openconnectors.token;

import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

public abstract class StreamingFeedSource implements PushSourceConnector<String> {

    private final Location location;
    private Consumer<Collection<String>> consumeFunction;
    private Socket socket;
    private String subscriptionId;
    private String exchangeId;
    private String sourceCurCode;
    private String targetCurCode;

    public StreamingFeedSource(Location location) {
        this.location = location;
    }

    @Override
    public void setConsumer(Consumer<Collection<String>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void initialize(ConnectorContext ctx) {

    }

    @Override
    public void open(Config config) throws Exception {

        subscriptionId = "2";
        exchangeId = location.EXCHANGE_NAME;
        sourceCurCode = getCurrencyPair().getSourceCurCode();
        targetCurCode = getCurrencyPair().getTargetCurCode();

        socket = IO.socket("https://streamer.cryptocompare.com/");
        socket.on(Socket.EVENT_CONNECT, new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                JSONArray jarr = new JSONArray();
                jarr.put(String.join(
                    CryptoCompareUtils.separator, subscriptionId, exchangeId, sourceCurCode, targetCurCode));
                JSONObject obj = new JSONObject();
                try {
                    obj.put("subs", jarr);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                socket.emit("SubAdd", obj);
            }
        }).on("m", new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                final String message = (String) args[0];
                String mask;
                String currency;
                String[] items = message.split(CryptoCompareUtils.separator);
                if (items[0].equals(subscriptionId)) {
                    mask = CryptoCompareUtils.hexStringToBinaryString(items[items.length - 1]);
                        LiveRate newLiveRate = CryptoCompareUtils.stringArrayToLiveRate(items, mask);
                        if (newLiveRate != null) {
                            consumeFunction.accept(Collections.singleton(newLiveRate.toString()));
                        }
                }
            }
        }).on(Socket.EVENT_DISCONNECT, (Object... args) -> System.out.println("EVENT_DISCONNECT"));

        socket.connect();
    }

    @Override
    public void close() {
        socket.close();
    }

    @Override
    public String getVersion() {
        return TokenConfigKeys.TOKEN_CONNECTOR_VERSION;
    }

    public abstract CurrencyPair getCurrencyPair();
}

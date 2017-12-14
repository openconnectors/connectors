package org.openconnectors.pulsar;

import org.apache.pulsar.shade.com.scurrilous.circe.Hash;
import org.openconnectors.PulsarSource;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.config.MapConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class PulsarSourceExample {
    private static final Logger LOG = Logger.getLogger(PulsarSourceExample.class.getName());

    public static void main(String[] args) {
        Consumer<Collection<byte[]>> pulsarConsumer = msgs -> {
            String messageAsString = new String(msgs.iterator().next());
            LOG.info(String.format("Message received from Pulsar: %s", messageAsString));
        };

        PulsarSource.Builder builder = PulsarSource.newBuilder()
                .setConsumerFunction(pulsarConsumer)
                .setSubscription("sub-1")
                .setBrokerUrl("pulsar://localhost:6650");

        PulsarSource pulsarSource1 = builder
                .setTopic("persistent://sample/standalone/ns1/example-topic1")
                .build();

        PulsarSource pulsarSource2 = builder
                .setTopic("persistent://sample/standalone/ns1/example-topic2")
                .build();

        PulsarSource pulsarSource3 = PulsarSource.newBuilder()
                .usingConfigProvider(new ConfigProvider())
                .build();

        Map<String, Object> pulsarSourceConfigMap = new HashMap<>() {{
            put("foo", "bar");
        }};

        PulsarSource pulsarSource4 = PulsarSource.newBuilder()
                .usingConfigProvider(new MapConfig(pulsarSourceConfigMap))
                .build();

        try {
            pulsarSource1.open();
            pulsarSource2.open();
            pulsarSource3.open();
        } catch (Exception e) {
            LOG.severe(e.getMessage());
        }
    }
}

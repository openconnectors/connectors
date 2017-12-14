package org.openconnectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

public class PulsarSource implements PushSourceConnector<byte[]> {
    private static final long serialVersionUID = 2611918383355492860L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSource.class.getName());
    private final String topic;
    private final String brokerUrl;
    private final String subscription;
    private java.util.function.Consumer<Collection<byte[]>> consumeFunction;

    private PulsarClient client;
    private org.apache.pulsar.client.api.Consumer consumer;

    @Override
    public void setConsumer(java.util.function.Consumer<Collection<byte[]>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void start() throws Exception {
        client = PulsarClient.create(brokerUrl);
        consumer = client.subscribe(topic, subscription);

        Thread runnerThread = new Thread(() -> {
            try {
                while (true) {
                    // Wait for a message`
                    Message msg = consumer.receive();
                    consumeFunction.accept(Collections.singleton(msg.getData()));

                    // Acknowledge the message so that it can be deleted by broker
                    consumer.acknowledgeAsync(msg);
                }
            } catch (PulsarClientException e) {
                LOG.error("Error receiving message from Pulsar consumer", e);
            }
        });
        runnerThread.setName("Pulsar Source thread");
        runnerThread.start();
    }

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }

        if (client != null) {
            client.close();
        }
    }

    @Override
    public String getVersion() {
        return PulsarConfig.PULSAR_CONNECTOR_VERSION;
    }

    private PulsarSource(Builder builder) {
        this.topic = builder.topic;
        this.brokerUrl = builder.brokerUrl;
        this.subscription = builder.subscription;
        setConsumer(builder.consumeFunction);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String topic;
        private String brokerUrl;
        private String subscription;
        private Consumer<Collection<byte[]>> consumeFunction;

        private Builder() {
            this.brokerUrl = PulsarConfig.Defaults.BROKER_URL;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setBrokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
            return this;
        }

        public Builder setSubscription(String subscription) {
            this.subscription = subscription;
            return this;
        }

        public Builder setConsumerFunction(Consumer<Collection<byte[]>> consumerFunction) {
            this.consumeFunction = consumerFunction;
            return this;
        }

        public Builder usingConfigProvider(Config config) {
            config.verify(
                    PulsarConfig.Keys.PULSAR_SOURCE_TOPIC,
                    PulsarConfig.Keys.PULSAR_SOURCE_SUBSCRIPTION
            );

            this.brokerUrl = (config.getString(PulsarConfig.Keys.PULSAR_SINK_BROKER_ROOT_URL) == null) ?
                    PulsarConfig.Defaults.BROKER_URL :
                    config.getString(PulsarConfig.Keys.PULSAR_SINK_BROKER_ROOT_URL);

            this.topic = config.getString(PulsarConfig.Keys.PULSAR_SOURCE_TOPIC);
            this.subscription = config.getString(PulsarConfig.Keys.PULSAR_SOURCE_SUBSCRIPTION);
            return this;
        }

        public PulsarSource build() {
            return new PulsarSource(this);
        }
    }
}

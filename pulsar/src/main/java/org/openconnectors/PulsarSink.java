package org.openconnectors;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public final class PulsarSink implements SinkConnector<byte[]> {
    private static final long serialVersionUID = -3640427018140871072L;
    private final String brokerUrl;
    private final String topic;

    private PulsarClient client;
    private Producer producer;
    private CompletableFuture<MessageId> messageIdCompletableFuture = null;

    @Override
    public CompletableFuture<Void> publish(Collection<byte[]> messages) {

        for (byte[] message : messages) {
            messageIdCompletableFuture = producer.sendAsync(message);
        }
        return messageIdCompletableFuture.thenApply(x -> null);
    }

    @Override
    public void flush() throws Exception {
        if (messageIdCompletableFuture != null) {
            messageIdCompletableFuture.join();
        }
    }

    @Override
    public void initialize(ConnectorContext ctx) {
    }

    @Override
    public void open() throws Exception {
        client = PulsarClient.create(brokerUrl);
        producer = client.createProducer(topic);
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (client != null) {
            client.close();
        }
    }

    @Override
    public String getVersion() {
        return PulsarConfig.PULSAR_CONNECTOR_VERSION;
    }

    private PulsarSink(Builder builder) {
        this.brokerUrl = builder.brokerUrl;
        this.topic = builder.topic;
    }

    public static final class Builder {
        private String brokerUrl;
        private String topic;

        public Builder setBrokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder usingConfigProvider(Config config) {
            this.brokerUrl = (config.getString(PulsarConfig.Keys.PULSAR_SINK_TOPIC) == null) ?
                    PulsarConfig.Defaults.BROKER_URL :
                    config.getString(PulsarConfig.Keys.PULSAR_SINK_TOPIC);

            config.verify(
                    PulsarConfig.Keys.PULSAR_SINK_TOPIC
            );

            this.topic = config.getString(PulsarConfig.Keys.PULSAR_SINK_TOPIC);
            return this;
        }

        public PulsarSink build() {
            return new PulsarSink(this);
        }
    }
}

package org.openconnectors;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.openconnectors.config.ConfigUtils.verifyExists;

public class PulsarSink implements SinkConnector<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSink.class);

    PulsarClient client;
    Producer producer;
    CompletableFuture<MessageId> messageIdCompletableFuture = null;

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
    public void open(Config config) throws Exception {
        verifyExists(config, PulsarConfigKeys.PULSAR_SINK_TOPIC);
        verifyExists(config, PulsarConfigKeys.PULSAR_SINK_BROKER_ROOT_URL);

        String pulsarBrokerRootUrl = config.getString(PulsarConfigKeys.PULSAR_SINK_BROKER_ROOT_URL);
        client = PulsarClient.create(pulsarBrokerRootUrl);
        String topic = config.getString(PulsarConfigKeys.PULSAR_SINK_TOPIC);

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
        return PulsarConfigKeys.PULSAR_CONNECTOR_VERSION;
    }
}

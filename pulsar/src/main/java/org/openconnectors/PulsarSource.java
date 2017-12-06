package org.openconnectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.openconnectors.config.Config;
import org.openconnectors.config.ConfigUtils;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

public class PulsarSource implements PushSourceConnector<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSource.class);

    private java.util.function.Consumer<Collection<byte[]>> consumeFunction;
    PulsarClient client;
    private org.apache.pulsar.client.api.Consumer consumer;

    @Override
    public void setConsumer(Consumer<Collection<byte[]>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void initialize(ConnectorContext ctx) {

    }

    @Override
    public void open(Config config) throws Exception {
        ConfigUtils.verifyExists(
                config,
                PulsarConfigKeys.PULSAR_SOURCE_TOPIC,
                PulsarConfigKeys.PULSAR_SOURCE_BROKER_ROOT_URL,
                PulsarConfigKeys.PULSAR_SOURCE_SUBSCRIPTION
        );

        String pulsarBrokerRootUrl = config.getString(PulsarConfigKeys.PULSAR_SOURCE_BROKER_ROOT_URL);
        client = PulsarClient.create(pulsarBrokerRootUrl);

        String topic = config.getString(PulsarConfigKeys.PULSAR_SOURCE_TOPIC);
        String subscription = config.getString(PulsarConfigKeys.PULSAR_SOURCE_SUBSCRIPTION);
        consumer = client.subscribe(topic, subscription);
        start();
    }

    private void start() {
        Thread runnerThread = new Thread(() -> {
            try {

                while (true) {
                    // Wait for a message
                    Message msg = consumer.receive();
                    consumeFunction.accept(Collections.singleton(msg.getData()));

                    // Acknowledge the message so that it can be deleted by broker
                    consumer.acknowledgeAsync(msg);
                }
            } catch (PulsarClientException e) {
                LOG.error("Error receiving message from pulsar consumer", e);
            }
        });
        runnerThread.setName("Pulsar Source Thread");
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
        return PulsarConfigKeys.PULSAR_CONNECTOR_VERSION;
    }
}

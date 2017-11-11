package org.openconnectors;

public class PulsarConfigKeys {

    private static final String PULSAR_SINK_PREFIX = "pulsar.sink.";
    private static final String PULSAR_SOURCE_PREFIX = "pulsar.source.";

    //Producer Configs
    public static final String PULSAR_SINK_TOPIC = PULSAR_SINK_PREFIX + "topic";
    public static final String PULSAR_SINK_BROKER_ROOT_URL = PULSAR_SINK_PREFIX + "broker.root.url";

    // Consumer Configs
    public static final String PULSAR_SOURCE_TOPIC = PULSAR_SOURCE_PREFIX + "topic";
    public static final String PULSAR_SOURCE_BROKER_ROOT_URL = PULSAR_SOURCE_PREFIX + "broker.root.url";
    public static final String PULSAR_SOURCE_SUBSCRIPTION = PULSAR_SOURCE_PREFIX + "subscription";

}

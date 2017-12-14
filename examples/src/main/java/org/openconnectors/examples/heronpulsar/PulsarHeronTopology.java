package org.openconnectors.examples.heronpulsar;

import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;
import com.twitter.heron.streamlet.Streamlet;
import org.apache.pulsar.client.api.SubscriptionType;
import org.openconnectors.PulsarSink;
import org.openconnectors.PulsarSource;
import org.openconnectors.pulsar.HeronPulsarSink;
import org.openconnectors.pulsar.HeronPulsarSource;

public class PulsarHeronTopology {
    public static void main(String[] args) {
        PulsarSource pulsarSource = PulsarSource.newBuilder()
                .setTopic("persistent://sample/standalone/ns1/source")
                .setSubscription("sub-1")
                .setSubscriptionType(SubscriptionType.Shared)
                .setAckTimeoutSeconds(10)
                .build();

        PulsarSink pulsarSink = PulsarSink.newBuilder()
                .setTopic("persistent://sample/standalone/ns1/sink")
                .build();

        Builder builder = Builder.createBuilder();
        Streamlet<byte[]> sourceStreamlet = builder.newSource(new HeronPulsarSource(pulsarSource));

        sourceStreamlet
                .map(bytes -> new String(bytes).toUpperCase().getBytes())
                .toSink(new HeronPulsarSink(pulsarSink));

        Config config = Config.defaultConfig();

        new Runner().run(args[0], config, builder);
    }
}
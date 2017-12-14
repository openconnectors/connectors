package org.openconnectors.examples.pulsar;

import org.openconnectors.PulsarSource;

public class PulsarSourceExample {
    public static void main(String[] args) throws Exception {
        final PulsarSource pulsarSource = PulsarSource.newBuilder()
                .setTopic("persistent://sample/standalone/ns1/test")
                .setSubscription("sub-1")
                .setConsumerFunction(msgs -> {
                    byte[] msg = msgs.stream().findFirst().orElse("UH OH".getBytes());
                    System.out.println(new String(msg));
                })
                .build();

        pulsarSource.start();
    }
}

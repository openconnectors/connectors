package org.openconnectors.pulsar;

import com.twitter.heron.streamlet.*;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSourceTopology {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceTopology.class);

    private PulsarSourceTopology() { }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();
        processingGraphBuilder.newSource(new HeronPulsarSource()).toSink(new Sink<byte[]>() {
            @Override
            public void setup(Context context) {

            }

            @Override
            public void put(byte[] tuple) {
                LOG.info(new String(tuple));
            }

            @Override
            public void cleanup() {

            }
        });
        Config config = Config.defaultConfig();
        new Runner().run("PulsarSourceTopology", config, processingGraphBuilder);
    }

}

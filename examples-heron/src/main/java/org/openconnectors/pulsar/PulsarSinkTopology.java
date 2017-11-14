package org.openconnectors.pulsar;

import com.twitter.heron.streamlet.*;

import static org.openconnectors.util.DataUtils.randomFromList;
import static org.openconnectors.util.DataUtils.sentences;

public class PulsarSinkTopology {
    private PulsarSinkTopology() { }

    public static void main(String[] args) throws Exception {

        Builder processingGraphBuilder = Builder.createBuilder();
        processingGraphBuilder
                .newSource((SerializableSupplier<byte[]>) () -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {

                    }
                    return randomFromList(sentences).getBytes();
                })
                .toSink(new HeronPulsarSink());
        Config config = Config.defaultConfig();
        new Runner().run("PulsarSinkTopology", config, processingGraphBuilder);

    }
}

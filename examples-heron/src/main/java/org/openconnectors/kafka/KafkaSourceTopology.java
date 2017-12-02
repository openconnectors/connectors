package org.openconnectors.kafka;

import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;

public class KafkaSourceTopology {

    private KafkaSourceTopology() { }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();
        processingGraphBuilder.newSource(new HeronKafka010Source<>()).log();
        Config config = Config.defaultConfig();
        new Runner().run("KafkaSourceTopology", config, processingGraphBuilder);
    }
}

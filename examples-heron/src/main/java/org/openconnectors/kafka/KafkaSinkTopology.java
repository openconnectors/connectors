package org.openconnectors.kafka;

import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Runner;

import static org.openconnectors.util.DataUtils.persons;
import static org.openconnectors.util.DataUtils.randomFromList;
import static org.openconnectors.util.DataUtils.sentences;

public class KafkaSinkTopology {

    private KafkaSinkTopology() { }

    public static void main(String[] args) throws Exception {

        Builder processingGraphBuilder = Builder.createBuilder();
        processingGraphBuilder.newSource(() -> new KeyValue<>(randomFromList(persons), randomFromList(sentences)))
            .toSink(new HeronKafka010Sink<>());
        Config config = Config.defaultConfig();
        new Runner().run("KafkaSinkTopology", config, processingGraphBuilder);

    }
}

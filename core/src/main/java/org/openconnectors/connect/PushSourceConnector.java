package org.openconnectors.connect;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Source Connector meant be the start of a DAG pipeline, after initilization is configured with a collector
 * and started with an execution instance
 *
 * @param <T> Type to messages to emitted by the source
 */
public interface PushSourceConnector<T> extends Connector {
    void setConsumer(Consumer<Collection<T>> consumeFunction);
}

package org.openconnectors.connect;

import java.util.Collection;

/**
 * Source Connector meant be the start of a DAG pipeline, after initilization is configured with a collector
 * and started with an execution instance
 *
 * @param <T> Type for the messages provided by the Connector
 */
public interface PullSourceConnector<T> extends Connector {
    Collection<T> fetch() throws Exception;
}

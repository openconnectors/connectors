package org.openconnectors.connect;

import java.util.Collection;

/**
 * Source Connector meant be the start of a DAG pipeline, after initilization is configured with a collector
 * and started with an execution instance
 *
 * @param <T>
 */
public interface SourceConnector<T> extends Connector {
    Collection<T> fetch() throws Exception;
}

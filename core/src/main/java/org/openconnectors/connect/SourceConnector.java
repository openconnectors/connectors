package org.openconnectors.connect;

import org.openconnectors.util.SourceConnectorContext;

import java.util.Collection;

public abstract class SourceConnector<T extends SourceConnectorContext, U> extends Connector<T> {

    public abstract Collection<U> poll() throws Exception;

    public abstract void start(SourceCollector<U> collector) throws Exception;

    public abstract void stop() throws Exception;

}

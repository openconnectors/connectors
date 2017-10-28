package org.openconnectors.connect;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source Connector meant be the start of a DAG pipeline, after initilization is configured with a collector
 * and started with an execution instance
 *
 * @param <T>
 * @param <U>
 */
public abstract class SourceConnector<U> extends Connector {

    private Collector<U> collector;
    private volatile AtomicBoolean isRunning;

    public void setCollector(final Collector<U> collector) throws Exception {
        this.collector = collector;
    }

    public Collector<U> getCollector(){
        return collector;
    }

    /**
     * Start the source connector
     *
     * @throws Exception
     */
    public abstract void run() throws Exception;

    /**
     * Pause the source connector
     *
     * @throws Exception
     */
    public abstract void pause() throws Exception;

    public AtomicBoolean isRunning(){
        return this.isRunning;
    }

}

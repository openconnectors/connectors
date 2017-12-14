/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.openconnectors.util;

import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PullSourceConnector;
import org.openconnectors.connect.PushSourceConnector;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Copy Topology template targeting unit testing
 */
public class PullWrapperOnPushSource<I> implements PullSourceConnector<I> {
    private static final long serialVersionUID = -6268410211096592480L;
    private PushSourceConnector<I> pushSource;
    private BufferStrategy bufferStrategy;
    private BlockingQueue<I> blockingQueue;

    public enum BufferStrategy {
        BLOCK
    }

    public PullWrapperOnPushSource(PushSourceConnector<I> source, int buffer, BufferStrategy bufferStrategy) {
        this.pushSource = source;
        this.bufferStrategy = bufferStrategy;
        this.blockingQueue = new LinkedBlockingQueue<>(buffer);
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        this.pushSource.initialize(ctx);
    }

    @Override
    public void close() throws Exception {
        this.pushSource.close();
    }

    @Override
    public Collection<I> fetch() throws Exception {
        Collection<I> out = new LinkedList<>();
        blockingQueue.drainTo(out);
        return out;
    }

    @Override
    public String getVersion() {
        return "0.0.1";
    }

    private void consume(Collection<I> messages) {
        if (messages != null) {
            for (I message : messages) {
                if (bufferStrategy == BufferStrategy.BLOCK) {
                    try {
                        blockingQueue.put(message);
                    } catch (InterruptedException e) {
                        // do something here
                    }
                } else {
                    blockingQueue.offer(message); // if this doesnt succeed, drop on the floor
                }
            }
        }
    }

}

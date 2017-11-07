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

package org.openconnectors.stdconnectors;

import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;

import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample Sink to print to std out
 *
 */
public class StdoutSink implements SinkConnector<String> {

    private String outputFormat;
    private AtomicLong linesReceived;
    private PrintStream stream = System.out;

    @Override
    public void initialize(ConnectorContext ctx) {
        // nothing really
    }

    @Override
    public CompletableFuture<Void> publish(Collection<String> messages) {
        return CompletableFuture.runAsync(() -> {
            for(String message : messages){
                long id = linesReceived.incrementAndGet();
                final String output = String.format(
                        outputFormat,
                        id,
                        message);
                stream.println(output);
            }
        });
    }

    @Override
    public void flush() throws Exception {
        stream.flush();
    }

    @Override
    public void open(Config config) throws Exception {
        outputFormat = config.getString(ConfigKeys.OUTPUT_FORMAT_KEY, ConfigKeys.DEFAULT_OUTPUT_FORMAT);
        stream = System.out;
        linesReceived = new AtomicLong(0);
    }

    @Override
    public void close() throws Exception {
        this.flush();
        stream.close();
    }

    @Override
    public String getVersion() {
        return StdStreamConVer.getVersion();
    }
}

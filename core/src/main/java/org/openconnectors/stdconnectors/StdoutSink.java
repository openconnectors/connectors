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
    private static final long serialVersionUID = -5695163797621079645L;
    private String outputFormat;
    private AtomicLong linesReceived  = new AtomicLong(0);
    private PrintStream stream = System.out;

    public StdoutSink() {
        this.outputFormat = StdConnectorsConfig.Defaults.OUTPUT_FORMAT;
    }

    public StdoutSink(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    private void receiveMessage(String message) {
        long id = linesReceived.incrementAndGet();
        linesReceived.incrementAndGet();
        final String output = String.format(
                outputFormat,
                id,
                message
        );
        stream.print(output);
    }

    @Override
    public CompletableFuture<Void> publish(Collection<String> messages) {
        return CompletableFuture.runAsync(() -> {
            messages.stream().forEach(msg -> receiveMessage(msg));
        });
    }

    @Override
    public void flush() throws Exception {
        stream.flush();
    }

    @Override
    public void close() throws Exception {
        this.flush();
        stream.close();
    }

    @Override
    public String getVersion() {
        return StdConnectorsConfig.STD_CONNECTORS_VERSION;
    }

    private StdoutSink(Builder builder) {
        outputFormat = builder.outputFormat;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String outputFormat;

        private Builder() {
            outputFormat = StdConnectorsConfig.Defaults.OUTPUT_FORMAT;
        }

        public Builder withOutputFormat(String outputFormat) {
            this.outputFormat = outputFormat;
            return this;
        }

        public Builder usingConfigProvider(Config config) {
            config.verify(
                    StdConnectorsConfig.Keys.OUTPUT_FORMAT_KEY
            );
            outputFormat = config.getString(StdConnectorsConfig.Keys.OUTPUT_FORMAT_KEY);
            return this;
        }

        public StdoutSink build() {
            return new StdoutSink(this);
        }
    }
}

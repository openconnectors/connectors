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
import org.openconnectors.connect.PullSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample Source connector polling std in a background thread will 100 ms poll cycle
 *
 */
public class StdinSource implements PullSourceConnector<String> {

    private static final Logger LOG = LoggerFactory.getLogger(StdinSource.class);

    private InputStream stream;
    private InputStreamReader streamReader;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;

    @Override
    public void open(Config config) throws Exception {
        stream = System.in;
        streamReader = new InputStreamReader(stream);
        bufferedReader = new BufferedReader(streamReader);
        linesRead = new AtomicLong(0);
    }

    @Override
    public Collection<String> fetch() throws Exception {
        if (bufferedReader.ready()) {
            try {
                linesRead.incrementAndGet();
                return Collections.singleton(bufferedReader.readLine());
            } catch (Exception e) {
                LOG.error("Received exception while reading from stdin");
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Source closed");
        bufferedReader.close();
        streamReader.close();
        stream.close();
    }

    @Override
    public String getVersion() {
        return StdStreamConVer.getVersion();
    }
}

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

package org.openconnectors.stream;

import org.apache.commons.lang3.NotImplementedException;
import org.openconnectors.config.Config;
import org.openconnectors.connect.Collector;
import org.openconnectors.connect.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sample Source connector polling std in a background thread will 100 ms poll cycle
 *
 * Lifecycle is init -> open -> run -> (pause if required) -> close
 *
 */
public class StdInputStreamSource extends SourceConnector<String> {

    private static final Logger LOG = LoggerFactory.getLogger(StdInputStreamSource.class);
    private static final long MAX_BACKOFF_SLEEP = 100;

    private InputStream stream;
    private InputStreamReader streamReader;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;
    private AtomicBoolean isRunning;
    private Thread runnerThread;
    private AtomicBoolean isStarted;

    @Override
    public void open(Config config) throws Exception {
        stream = System.in;
        streamReader = new InputStreamReader(stream);
        bufferedReader = new BufferedReader(streamReader);
        linesRead = new AtomicLong(0);
        isRunning = new AtomicBoolean();
        isStarted = new AtomicBoolean();
        runnerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.info("Polling Source opened");
                while (true) {
                    try {
                        if (bufferedReader.ready()) {
                            linesRead.incrementAndGet();
                            getCollector().collect(Collections.singleton(bufferedReader.readLine()));
                        }
                        Thread.sleep(MAX_BACKOFF_SLEEP);
                    } catch (Exception e) {
                        LOG.error("Unhandled exception, logging and sleeping for " + MAX_BACKOFF_SLEEP + "ms", e);
                        try {
                            Thread.sleep(MAX_BACKOFF_SLEEP);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }

        });
        runnerThread.setName("SourceRunner");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Source closed");
        runnerThread.interrupt();
        runnerThread.join();
        bufferedReader.close();
        streamReader.close();
        stream.close();
    }

    @Override
    public void pause() throws Exception {
        LOG.info("Source paused");
        if (isRunning.get() != false) {
            isRunning.set(false);
            runnerThread.wait();
        }
    }

    @Override
    public String getVersion() {
        return StdStreamConVer.getVersion();
    }

    @Override
    public void run() throws Exception {
        LOG.info("Source started");
        if(isStarted.get() == false){
            synchronized (runnerThread) {
                runnerThread.start();
            }
            isStarted.set(true);
            isRunning.set(true);
        }
        if (isRunning.get() == false) {
            isRunning.set(true);
            synchronized (runnerThread) {
                runnerThread.notify();
            }
        }
    }
}

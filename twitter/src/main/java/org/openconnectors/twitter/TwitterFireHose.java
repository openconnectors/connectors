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

package org.openconnectors.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Sample Source connector polling std in a background thread will 100 ms poll cycle
 *
 * Lifecycle is init -> open -> run -> (pause if required) -> close
 *
 */
public class TwitterFireHose implements PushSourceConnector<String> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterFireHose.class);
    public static final String CONSUMER_KEY = "twitter-source.consumerKey";

    public static final String CONSUMER_SECRET = "twitter-source.consumerSecret";

    public static final String TOKEN = "twitter-source.token";

    public static final String TOKEN_SECRET = "twitter-source.tokenSecret";

    // ------ Optional property keys

    public static final String CLIENT_NAME = "twitter-source.name";

    public static final String CLIENT_HOSTS = "twitter-source.hosts";

    public static final String CLIENT_BUFFER_SIZE = "twitter-source.bufferSize";

    // ----- Fields set by the constructor

    // ----- Runtime fields
    private Object waitObject;
    private Consumer<Collection<String>> consumeFunction;


    @Override
    public void initialize(ConnectorContext ctx) {
        // Nothing really
    }

    @Override
    public void open(Config config) throws Exception {
        verifyExists(config, CONSUMER_KEY);
        verifyExists(config, CONSUMER_SECRET);
        verifyExists(config, TOKEN);
        verifyExists(config, TOKEN_SECRET);
        waitObject = new Object();
        startThread(config);
    }

    @Override
    public void setConsumer(Consumer<Collection<String>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void close() throws Exception {
        stopThread();
    }

    @Override
    public String getVersion() {
        return "0.0.1";
    }

    // ------ Custom endpoints

    /**
     * Implementing this interface allows users of this source to set a custom endpoint.
     */
    public interface EndpointInitializer {
        StreamingEndpoint createEndpoint();
    }

    /**
     * Default endpoint initializer returning the {@see StatusesSampleEndpoint}.
     */
    private static class SampleStatusesEndpoint implements EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {
            // this default endpoint initializer returns the sample endpoint: Returning a sample from the firehose (all tweets)
            StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
            endpoint.stallWarnings(false);
            endpoint.delimited(false);
            return endpoint;
        }
    }

    private void startThread(Config config) {
        Authentication auth = new OAuth1(config.getString(CONSUMER_KEY),
                config.getString(CONSUMER_SECRET),
                config.getString(TOKEN),
                config.getString(TOKEN_SECRET));

        BasicClient client = new ClientBuilder()
                .name(config.getString(CLIENT_NAME, "openconnector-twitter-source"))
                .hosts(config.getString(CLIENT_HOSTS, Constants.STREAM_HOST))
                .endpoint(new SampleStatusesEndpoint().createEndpoint())
                .authentication(auth)
                .processor(new HosebirdMessageProcessor() {
                    public DelimitedStreamReader reader;

                    @Override
                    public void setup(InputStream input) {
                        reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET, Integer.parseInt(config.getString(CLIENT_BUFFER_SIZE, "50000")));
                    }

                    @Override
                    public boolean process() throws IOException, InterruptedException {
                        String line = reader.readLine();
                        try {
                            consumeFunction.accept(Collections.singleton(line));
                        } catch (Exception e) {
                            LOG.error("Exception thrown");
                        }
                        return true;
                    }
                })
                .build();

        Thread runnerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.info("Started the Twitter FireHose Runner Thread");
                client.connect();
                LOG.info("Twitter Streaming API connection established successfully");

                // just wait now
                try {
                    synchronized (waitObject) {
                        waitObject.wait();
                    }
                } catch (Exception e) {
                    LOG.info("Got a exception in waitObject");
                }
                LOG.debug("Closing Twitter Streaming API connection");
                client.stop();
                LOG.info("Twitter Streaming API connection closed");
                LOG.info("Twitter FireHose Runner Thread ending");
            }
        });
        runnerThread.setName("TwitterFireHoseRunner");
        runnerThread.start();
    }

    private void stopThread() throws Exception {
        LOG.info("Source closed");
        synchronized (waitObject) {
            waitObject.notify();
        }
    }

    private void verifyExists(Config config, String key) {
        if (config.getString(key) == null) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }
}

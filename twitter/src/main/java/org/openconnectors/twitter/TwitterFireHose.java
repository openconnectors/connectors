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

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.common.DelimitedStreamReader;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.openconnectors.connect.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import static org.openconnectors.twitter.TwitterFireHoseConfig.Defaults;
import static org.openconnectors.twitter.TwitterFireHoseConfig.TWITTER_CONNECTOR_VERION;

/**
 * Simple Push based Twitter FireHose Source
 */
public class TwitterFireHose implements Connector {
    private static final long serialVersionUID = -5847155241884298914L;
    private static final Logger LOG = LoggerFactory.getLogger(TwitterFireHose.class);
    private final String consumerKey, consumerSecret, token, tokenSecret, clientName, hosts;
    private final Object waitObject;
    private final Consumer<Collection<String>> consumeFunction;
    private final int clientBufferSize;

    @Override
    public void close() throws Exception {
        stopThread();
    }

    @Override
    public String getVersion() {
        return TWITTER_CONNECTOR_VERION;
    }

    // ------ Custom endpoints

    /**
     * Implementing this interface allows users of this source to set a custom endpoint.
     */
    public interface EndpointInitializer {
        StreamingEndpoint createEndpoint();
    }

    /**
     * Required for Twitter Client
     */
    private static class SampleStatusesEndpoint implements EndpointInitializer, Serializable {
        private static final long serialVersionUID = 6307553368518507738L;

        @Override
        public StreamingEndpoint createEndpoint() {
            // this default endpoint initializer returns the sample endpoint: Returning a sample from the firehose (all tweets)
            StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
            endpoint.stallWarnings(false);
            endpoint.delimited(false);
            return endpoint;
        }
    }

    @Override
    public void start() {
        Authentication auth = new OAuth1(
                consumerKey,
                consumerSecret,
                token,
                tokenSecret);

        BasicClient client = new ClientBuilder()
                .name(clientName)
                .hosts(hosts)
                .endpoint(new SampleStatusesEndpoint().createEndpoint())
                .authentication(auth)
                .processor(new HosebirdMessageProcessor() {
                    public DelimitedStreamReader reader;

                    @Override
                    public void setup(InputStream input) {
                        reader = new DelimitedStreamReader(input, Constants.DEFAULT_CHARSET, clientBufferSize);
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

        Thread runnerThread = new Thread(() -> {
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

    public static Builder newBuilder() {
        return new Builder();
    }

    private TwitterFireHose(Builder builder) {
        consumerKey = builder.consumerKey;
        consumerSecret = builder.consumerSecret;
        token = builder.token;
        tokenSecret = builder.tokenSecret;
        consumeFunction = builder.consumeFunction;
        clientName = builder.clientName;
        hosts = builder.hosts;
        clientBufferSize = builder.clientBufferSize;
        waitObject = new Object();
    }

    public static final class Builder {
        private String consumerKey, consumerSecret, token, tokenSecret, clientName, hosts;
        private int clientBufferSize;
        private Consumer<Collection<String>> consumeFunction;

        private Builder() {
            this.clientName = Defaults.CLIENT_NAME;
            this.hosts = Defaults.CLIENT_HOSTS;
            this.clientBufferSize = Defaults.CLIENT_BUFFER_SIZE;
        }

        public Builder setConsumerKey(String consumerKey) {
            this.consumerKey = consumerKey;
            return this;
        }

        public Builder setConsumerSecret(String consumerSecret) {
            this.consumerSecret = consumerSecret;
            return this;
        }

        public Builder setToken(String token) {
            this.token = token;
            return this;
        }

        public Builder setTokenSecret(String tokenSecret) {
            this.tokenSecret = tokenSecret;
            return this;
        }

        public Builder setClientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        public Builder setConsumeFunction(Consumer<Collection<String>> consumeFunction) {
            this.consumeFunction = consumeFunction;
            return this;
        }

        public Builder setHosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder setClientBufferSize(int clientBufferSize) {
            this.clientBufferSize = clientBufferSize;
            return this;
        }

        public TwitterFireHose build() {
            return new TwitterFireHose(this);
        }
    }
}

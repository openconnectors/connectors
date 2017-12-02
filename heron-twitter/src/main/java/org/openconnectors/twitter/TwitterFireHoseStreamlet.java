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

import java.util.Collection;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Source;
import org.openconnectors.config.ConfigProvider;
import org.openconnectors.util.PullWrapperOnPushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Push based Twitter FireHose Source
 */
public class TwitterFireHoseStreamlet implements Source<String> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterFireHoseStreamlet.class);
    PullWrapperOnPushSource<String> pullHose;

    @Override
    public void setup(Context ctx) {
        if (pullHose == null) {
            TwitterFireHose hose = new TwitterFireHose();
            pullHose = new PullWrapperOnPushSource<> (hose, 1024 * 1024,
                    PullWrapperOnPushSource.BufferStrategy.BLOCK);
        }
        try {
            pullHose.open(new ConfigProvider());
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Twitter Source", e);
        }
    }

    @Override
    public Collection<String> get() {
        try {
            return pullHose.fetch();
        } catch (Exception e) {
            LOG.error("Exception thrown while fetching from Twitter Source");
        }
        return null;
    }

    @Override
    public void cleanup() {
        try {
            pullHose.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Twitter FireHose");
        }
    }
}

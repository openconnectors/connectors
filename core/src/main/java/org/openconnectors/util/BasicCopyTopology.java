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

import org.openconnectors.config.Config;
import org.openconnectors.connect.SourceConnector;

/**
 * Copy Topology template targeting unit testing
 */
public class BasicCopyTopology<T> {

    private SourceConnector<T> source;
    private SinkCollector<T> sinkCollector;

    public BasicCopyTopology(SourceConnector<T> source, SinkCollector<T> sink) {
        this.source = source;
        this.sinkCollector = sink;
    }

    public void setup(Config config) throws Exception {
        this.source.open(config);
        this.sinkCollector.getSink().open(config);
        this.source.setCollector(this.sinkCollector);
    }

    public void run() throws Exception {
        this.source.run();
    }

}

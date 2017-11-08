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

package org.openconnectors.aerospike;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdinSource;
import org.openconnectors.util.KeyValue;
import org.openconnectors.util.SimpleCopier;

/**
 * Basic topology to copy data fro stdin to aerospike, useful for experimentation
 */
public class StdInToAerospikeCopier extends SimpleCopier<String, KeyValue<String, String>> {

    public StdInToAerospikeCopier() {
        super(new StdinSource(), new AerospikeSink<>(), x -> new KeyValue<>(x, x));
    }

    public static void main(String[] args) throws Exception {
        StdInToAerospikeCopier instance = new StdInToAerospikeCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}

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

package org.openconnectors.stdinstdout;

import org.openconnectors.config.ConfigProvider;
import org.openconnectors.stdconnectors.StdinSource;
import org.openconnectors.stdconnectors.StdoutSink;
import org.openconnectors.util.SimpleCopier;

/**
 * Basic topology to copy data from stdin to std out, useful for experimentation
 */
public class StdInToStdOutCopier extends SimpleCopier<String, String> {

    public StdInToStdOutCopier() {
        super(new StdinSource(), new StdoutSink(), x -> x);
    }

    public static void main(String[] args) throws Exception {
        StdInToStdOutCopier instance = new StdInToStdOutCopier();
        instance.setup(new ConfigProvider());
        instance.run();
    }
}

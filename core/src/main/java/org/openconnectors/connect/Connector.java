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

package org.openconnectors.config;

import org.openconnectors.config.Config;
import org.openconnectors.util.ConnectorContext;
import org.openconnectors.util.Versionable;

import java.io.Closeable;
import java.io.Serializable;

public abstract class Connector<T extends ConnectorContext> implements Versionable, Closeable, Serializable {

    private T context;

    public void initialize(T ctx){
        this.context = ctx;
    }

    public abstract void open(Config config) throws Exception;

    public void reset(Config config) throws Exception{
        close();
        open(config);
    }

    public T getContext() {
        return context;
    }
}
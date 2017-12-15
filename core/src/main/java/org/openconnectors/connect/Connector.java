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

package org.openconnectors.connect;

import java.io.Serializable;

/**
 * Connector component base abstract class
 * The lifecycle of a connector is to initialize with a connector context,
 * open it for processing, and then close it at the end of the session
 */
public interface Connector extends AutoCloseable, Versionable, Serializable {
    /**
     * First method to call in the lifecycle of the object
     *
     * @param ctx Connector context for distributed runtimes
     */
    default void initialize(final ConnectorContext ctx) {
    }

    default void start() throws Exception {
    }
}
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

import org.openconnectors.connect.Schema.SchemaManager;
import org.openconnectors.connect.events.Listener;
import org.openconnectors.connect.events.SendableEvent;

import java.util.UUID;

/**
 * ConnectorContext is a construct provided to each connector instance to abstract away the underlying runtime
 * capabilities provided are a state manager, partition management, schema registry interaction etc.
 */
public interface ConnectorContext {


    /**
     * Get a reference to a global state manager for state coordination, dedup and ordering semantics
     *
     * @return global state manager instance
     */
    StateManager getStateManager();

    /**
     * Get a Globally unique connector instance identifier for the connector instance
     *
     * @return globally unique uuid
     */
    UUID getConnectorId();

    /**
     * Partition id for the connector instance.
     *
     * @return local partition id in cluster
     */
    int getPartitionId();

    /**
     * Get expected total instances of this connector type currently active
     *
     * @return total active instance count
     */
    int getInstanceCount();

    /**
     * Notifying the external runtime of metrics and other related events
     *
     * @param event Event to Sent
     * @return Error or success code
     */
    Integer publishEvent(SendableEvent event);

    /**
     * Receiving events from an external manager post initialization, meant for throttling and config updates.
     *
     * @param listener Local listener object to bind to.
     */
    void subscribeToEvents(Listener listener);

    /**
     * Get a reference to the global state manager
     *
     * @return a client object to interact with the scheme registry
     */
    SchemaManager getSchemaManager();
}

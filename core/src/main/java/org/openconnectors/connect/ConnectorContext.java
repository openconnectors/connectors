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

public interface ConnectorContext {

    // State Manager for state coordination, dedup and ordering semantics
    StateManager getStateManageger();

    // Globally unique connector instance identifier
    UUID getConnectorId();

    // Partition Id for this context
    int getPartitionId();

    // Expected total instances of this connector type currently active
    int getInstanceCount();

    // Notifying the external runtime of metrics and other related events
    Integer publishEvent(SendableEvent event);

    // Receiving events from an external manager post initialization
    // ie: dynamic reconfiguration
    void subscribeToEvents(Listener listener);

    // Get schema manager
    SchemaManager getSchemaManager();
}

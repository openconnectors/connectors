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

import java.util.Set;

/**
 * Base Config Interface
 */
public interface Config {

    String getString(final String propertyName);
    Integer getInt(final String propertyName);
    Long getLong(final String propertyName);
    Double getDouble(final String propertyName);
    Boolean getBoolean(final String propertyName);
    Object getObject(final String propertyName);

    String getString(final String propertyName, final String defaultValue);
    Integer getInt(final String propertyName, final Integer defaultValue);
    Long getLong(final String propertyName, final Long defaultValue);
    Double getDouble(final String propertyName, final Double defaultValue);
    Boolean getBoolean(final String propertyName, final Boolean defaultValue);
    Object getObject(final String propertyName, final Object defaultValue);

    Set<String> getPropertyKeys();
}

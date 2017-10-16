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

package org.streamlio.config;

import java.util.HashMap;
import java.util.Set;

public class MapConfig implements Config {

    final HashMap<String, Object> properties;

    public MapConfig(){
        properties = new HashMap<>();
    }

    public MapConfig(final HashMap<String, Object> props){
        properties = props;
    }

    @Override
    public String getString(String propertyName) {
        if (isNull(propertyName, properties)) return null;
        return properties.get(propertyName).toString();
    }

    @Override
    public Integer getInt(String propertyName) {
        if (isNull(propertyName, properties)) return null;
        return Integer.parseInt(properties.get(propertyName).toString());
    }

    @Override
    public Long getLong(String propertyName) {
        if (isNull(propertyName, properties)) return null;
        return Long.parseLong(properties.get(propertyName).toString());
    }

    @Override
    public Double getDouble(String propertyName) {
        if (isNull(propertyName, properties)) return null;
        return Double.parseDouble(properties.get(propertyName).toString());
    }

    @Override
    public Boolean getBoolean(String propertyName) {
        if (isNull(propertyName, properties)) return null;
        return Boolean.parseBoolean(properties.get(propertyName).toString());
    }

    @Override
    public Object getObject(String propertyName) {
        return properties.get(propertyName).toString();
    }

    @Override
    public String getString(String propertyName, String defaultValue) {
        return (getString(propertyName) != null) ? getString(propertyName) : defaultValue;
    }

    @Override
    public Integer getInt(String propertyName, Integer defaultValue) {
        return (getInt(propertyName) != null) ? getInt(propertyName) : defaultValue;
    }

    @Override
    public Long getLong(String propertyName, Long defaultValue) {
        return (getLong(propertyName) != null) ? getLong(propertyName) : defaultValue;
    }

    @Override
    public Double getDouble(String propertyName, Double defaultValue) {
        return (getDouble(propertyName) != null) ? getDouble(propertyName) : defaultValue;
    }

    @Override
    public Boolean getBoolean(String propertyName, Boolean defaultValue) {
        return (getBoolean(propertyName) != null) ? getBoolean(propertyName) : defaultValue;
    }

    @Override
    public Object getObject(String propertyName, Object defaultValue) {
        return (getObject(propertyName) != null) ? getObject(propertyName) : defaultValue;
    }

    private static boolean isNull(String propertyName, HashMap<String, Object> properties) {
        if(properties == null || propertyName == null || properties.containsKey(propertyName) == false){
            return true;
        }
        return false;
    }

    @Override
    public Set<String> getPropertyKeys() {
        if(properties == null){
            return null;
        }
        return properties.keySet();
    }
}

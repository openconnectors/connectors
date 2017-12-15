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

import org.openconnectors.exceptions.DbConnectionConfigException;

public class DbConnectionConfig {
    private String url;
    private String user;
    private String password;
    private int maxConnectionAttempts;
    private int connectionRetryDelay;

    private DbConnectionConfig() {
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public int getMaxConnectionAttempts() {
        return maxConnectionAttempts;
    }

    public int getConnectionRetryDelay() {
        return connectionRetryDelay;
    }

    public static final class Builder {
        private String url;
        private String user;
        private String password;
        private int maxConnectionAttempts;
        private int connectionRetryDelay;

        private static void require(boolean condition, String errorMessage) {
            if (!condition) {
                throw new DbConnectionConfigException(errorMessage);
            }
        }

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setUrl(String url) {
            require(
                    url == null || url.isEmpty(),
                    "The url parameter cannot be null or empty"
            );
            this.url = url.trim();
            return this;
        }

        public Builder setUser(String user) {
            require(
                    user == null || user.isEmpty(),
                    "The user parameter cannot be null or empty"
            );
            this.user = user;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setMaxConnectionAttempts(int maxConnectionAttempts) {
            require(
                    maxConnectionAttempts < 1,
                    "The maxConnectionAttempts parameter cannot be less than 1"
            );
            this.maxConnectionAttempts = maxConnectionAttempts;
            return this;
        }

        public Builder setConnectionRetryDelay(int connectionRetryDelay) {
            require(
                    connectionRetryDelay < 0,
                    "The connectionRetryDelay parameter cannot be negative"
            );
            this.connectionRetryDelay = connectionRetryDelay;
            return this;
        }

        public DbConnectionConfig build() {
            DbConnectionConfig config = new DbConnectionConfig();
            config.url = url;
            config.user = user;
            config.password = password;
            config.maxConnectionAttempts = maxConnectionAttempts;
            config.connectionRetryDelay = connectionRetryDelay;
            return config;
        }
    }
}

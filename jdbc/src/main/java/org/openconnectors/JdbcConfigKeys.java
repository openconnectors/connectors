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

package org.openconnectors;

public class JdbcConfigKeys {
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    public static final String CONNECTION_USER_CONFIG = "connection.user";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    public static final String CONNECTION_MAX_ATTEMPT = "connection.max.attempt";
    public static final String CONNECTION_RETRY_DELAY = "connection.retry.delay";
    public static final String SCHEMA_PATTERN = "schema.pattern";
    public static final String MODE = "mode";
    public static final String BULK_MODE = "mode.bulk";
    public static final String AUTOINCREMENTING_MODE= "mode.autoincrementing";
    public static final String WHITE_LIST_TABLES = "white.list.tables";
    public static final String BLACK_LIST_TABLES = "black.list.tables";
    public static final String TABLE_CHECK_PERIOD = "table.check.period";
}

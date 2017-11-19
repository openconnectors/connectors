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
    /**
     * JDBC connection URL
     */
    public static final String CONNECTION_URL_CONFIG = "connection.url";

    /**
     * Database user
     */
    public static final String CONNECTION_USER_CONFIG = "connection.user";

    /**
     * Database password
     */
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";

    /**
     * Maximum number of retry to connect to the database.
     */
     public static final String CONNECTION_MAX_ATTEMPT = "connection.max.attempt";

     /**
     * Delay between retries to connect to the database.
     */
    public static final String CONNECTION_RETRY_DELAY = "connection.retry.delay";

    /**
     * Schema pattern.
     */
    public static final String SCHEMA_PATTERN = "schema.pattern";

    /**
     * Mode of querying. Can be `mode.bulk`, `mode.autoincrementing`, `mode.incrementing`, `mode.by_query`.
     */
    public static final String MODE = "mode";

    /**
     * In the bulk mode the whole table is returned. It can be queried only once or periodically.
     * By default in bulk mode table is queried only once. In order to make it query periodically
     * time period in seconds must be set to `table.check.period` parameter.
     */
    public static final String BULK_MODE = "mode.bulk";

    /**
     * In the autoincrementing mode only new records are returned. A new record is selected by
     * a column in the table that is generated automatically. Usually it is a ID column of type Long.
     */
    public static final String AUTOINCREMENTING_MODE= "mode.autoincrementing";

    // TODO: add description of other modes.

    /**
     * A list of the tables and columns that must be handled.
     *
     * Tables must be specified in one line one by one separated by comma.
     * In order to query all of the particular table set empty brackets or brackets with star symbol
     * after table name. Find some valid examples of this parameter below:
     *  - table_name_1(column_name1, column_name2), table_name_2(*), table_name_3()
     *
     */
    public static final String WHITE_LIST_TABLES = "table.whitelist";

    /**
     * A list of tables that must be ignored.
     *
     * Tables must be specified in on line one by one separated by comma. No brackets are allowed.
     * Ignoring only some specific columns in a table is not supported. Find some valid examples
     * of the parameter below:
     * - table_name_1, table_name2, table_name3
     *
     */
    public static final String BLACK_LIST_TABLES = "table.blacklist";

    /**
     * Period of time in seconds of the delay between checking of the tables.
     */
    public static final String TABLE_CHECK_PERIOD = "table.check.period";
}

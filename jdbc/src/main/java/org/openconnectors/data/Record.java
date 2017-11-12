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

package org.openconnectors.data;

import java.io.Serializable;

public class Record implements Serializable{
    private Object[] values;
    private TableMetaData tableMetaData;

    public Record(Object[] values, TableMetaData tableMetaData) {
        this.values = values;
        this.tableMetaData = tableMetaData;
    }

    public Object[] getValues() {
        return values;
    }

    public TableMetaData getTableMetaData() {
        return tableMetaData;
    }
}

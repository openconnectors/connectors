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

package org.openconnectors.sink;

import org.openconnectors.exceptions.EmptyColumnsListException;

import java.util.List;
import java.util.stream.Collectors;

public class GenericDbDialect implements DbDialect {

    private String escapeStart;
    private String escapeEnd;

    public GenericDbDialect() {
    }

    public GenericDbDialect(String escapeSymbol) {
        escapeStart = escapeEnd = escapeSymbol;
    }

    public GenericDbDialect(String escapeStartSymbol, String escapeEndSymbol) {
        escapeStart = escapeStartSymbol;
        escapeEnd= escapeEndSymbol;
    }

    @Override
    public String getInsertStatement(String tableName, List<String> columns) {
        if (columns.isEmpty()) {
            throw new EmptyColumnsListException("Unable to create insert statement for empty list of columns.");
        }
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        if (escapeStart != null) {
            builder.append(escapeStart);
        }
        builder.append(tableName);
        if (escapeEnd != null) {
            builder.append(escapeEnd);
        }
        builder.append("(");
        builder.append(String.join(", ", columns));
        builder.append(") VALUES(");
        List<String> parameters = columns.stream().map(c -> "?").collect(Collectors.toList());
        builder.append(String.join(", ", parameters));
        builder.append(")");

        return builder.toString().toUpperCase().trim();
    }

}

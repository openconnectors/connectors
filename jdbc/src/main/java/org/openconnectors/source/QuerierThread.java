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

package org.openconnectors.source;

import org.openconnectors.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class QuerierThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(QuerierThread.class);
    private TableQuerier tableQuerier;
    private long queryPeriodSeconds;
    private AtomicBoolean isActive = new AtomicBoolean(false);
    private Consumer<Collection<Record>> consumeFunction;

    public QuerierThread(TableQuerier tableQuerier, long queryPeriodSeconds, Consumer<Collection<Record>> consumeFunction) {
        this.tableQuerier = tableQuerier;
        this.queryPeriodSeconds = queryPeriodSeconds;
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void run() {
        isActive.set(true);
        while (isActive.get()) {
            try {
                List<Record> records = tableQuerier.query();
                consumeFunction.accept(records);
            } catch (SQLException e) {
                // TODO: add log message
                logger.error("", e);
            }
            try {
                Thread.sleep(queryPeriodSeconds * 1000);
            } catch (InterruptedException e) {
                // TODO: add log message
                logger.error("", e);
            }
        }
    }

    public void stopQuerying() {
        isActive.set(false);
    }
}

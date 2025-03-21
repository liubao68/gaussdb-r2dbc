/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.gaussdb;

import io.r2dbc.gaussdb.util.Assert;
import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link Batch} for executing a collection of statements in a batch against a GaussDB database.
 */
final class GaussDBBatch implements io.r2dbc.gaussdb.api.GaussDBBatch {

    private final ConnectionResources context;

    private final List<String> statements = new ArrayList<>();

    GaussDBBatch(ConnectionResources context) {
        this.context = Assert.requireNonNull(context, "context must not be null");
    }

    @Override
    public GaussDBBatch add(String sql) {
        Assert.requireNonNull(sql, "sql must not be null");

        if (!(PostgresqlSqlParser.parse(sql).getParameterCount() == 0)) {
            throw new IllegalArgumentException(String.format("Statement '%s' is not supported.  This is often due to the presence of parameters.", sql));
        }

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<io.r2dbc.gaussdb.api.PostgresqlResult> execute() {
        return new PostgresqlStatement(this.context, String.join("; ", this.statements))
            .execute();
    }

    @Override
    public String toString() {
        return "PostgresqlBatch{" +
            "context=" + this.context +
            ", statements=" + this.statements +
            '}';
    }

}

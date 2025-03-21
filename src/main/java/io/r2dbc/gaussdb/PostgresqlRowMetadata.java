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

import io.r2dbc.gaussdb.codec.Codecs;
import io.r2dbc.gaussdb.message.backend.RowDescription;
import io.r2dbc.gaussdb.util.Assert;
import io.r2dbc.spi.RowMetadata;
import reactor.util.annotation.Nullable;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An implementation of {@link RowMetadata} for a PostgreSQL database.
 */
final class PostgresqlRowMetadata extends AbstractCollection<String> implements io.r2dbc.gaussdb.api.PostgresqlRowMetadata {

    private final List<GaussDBColumnMetadata> columnMetadatas;

    private final Map<String, GaussDBColumnMetadata> nameKeyedColumns;

    private final Map<String, Integer> columnNameIndexMap;

    PostgresqlRowMetadata(List<GaussDBColumnMetadata> columnMetadatas) {
        this.columnMetadatas = Assert.requireNonNull(columnMetadatas, "columnMetadatas must not be null");
        this.nameKeyedColumns = new LinkedHashMap<>(columnMetadatas.size(), 1);
        this.columnNameIndexMap = new HashMap<>(columnMetadatas.size(), 1);

        int i = 0;
        for (GaussDBColumnMetadata columnMetadata : columnMetadatas) {
            this.nameKeyedColumns.putIfAbsent(columnMetadata.getName(), columnMetadata);
            this.columnNameIndexMap.putIfAbsent(columnMetadata.getName().toLowerCase(Locale.ROOT), i++);
        }
    }

    @Override
    public GaussDBColumnMetadata getColumnMetadata(int index) {
        if (index >= this.columnMetadatas.size()) {
            throw new IndexOutOfBoundsException(String.format("Column index %d is larger than the number of columns %d", index, this.columnMetadatas.size()));
        }

        return this.columnMetadatas.get(index);
    }

    @Override
    public GaussDBColumnMetadata getColumnMetadata(String name) {
        Assert.requireNonNull(name, "name must not be null");

        for (GaussDBColumnMetadata metadata : this.columnMetadatas) {

            if (metadata.getName().equalsIgnoreCase(name)) {
                return metadata;
            }
        }

        throw new NoSuchElementException(String.format("Column name '%s' does not exist in column names %s", name, this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PostgresqlRowMetadata that = (PostgresqlRowMetadata) o;
        return Objects.equals(this.columnMetadatas, that.columnMetadatas);
    }

    @Override
    public List<GaussDBColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(this.columnMetadatas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.columnMetadatas);
    }

    @Override
    public int size() {
        return this.columnMetadatas.size();
    }

    @Override
    public boolean contains(Object o) {

        if (o instanceof String) {
            return this.findColumn((String) o) != null;
        }

        return false;
    }

    @Override
    public Iterator<String> iterator() {

        return new Iterator<String>() {

            int index = 0;

            @Override
            public boolean hasNext() {
                return size() > this.index;
            }

            @Override
            public String next() {
                return PostgresqlRowMetadata.this.columnMetadatas.get(this.index++).getName();
            }
        };
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * Lookup {@link GaussDBColumnMetadata} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link GaussDBColumnMetadata}.
     */
    @Nullable
    GaussDBColumnMetadata findColumn(String name) {

        GaussDBColumnMetadata column = this.nameKeyedColumns.get(name);

        if (column == null) {
            name = EscapeAwareColumnMatcher.findColumn(name, this.nameKeyedColumns.keySet());
            if (name != null) {
                column = this.nameKeyedColumns.get(name);
            }
        }

        return column;
    }

    Map<String, Integer> getColumnNameIndexMap() {
        return this.columnNameIndexMap;
    }

    static PostgresqlRowMetadata toRowMetadata(Codecs codecs, RowDescription rowDescription) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(rowDescription, "rowDescription must not be null");

        return new PostgresqlRowMetadata(getColumnMetadatas(codecs, rowDescription));
    }

    private static List<GaussDBColumnMetadata> getColumnMetadatas(Codecs codecs, RowDescription rowDescription) {
        List<GaussDBColumnMetadata> columnMetadatas = new ArrayList<>(rowDescription.getFields().size());

        for (RowDescription.Field field : rowDescription.getFields()) {
            columnMetadatas.add(GaussDBColumnMetadata.toColumnMetadata(codecs, field));
        }

        return columnMetadatas;
    }

}

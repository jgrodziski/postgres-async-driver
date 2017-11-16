/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl;

import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import lombok.AllArgsConstructor;

import java.util.*;

/**
 * {@link ResultSet} constructed from Query/Execute response messages.
 *
 * @author Antti Laisi
 */
@AllArgsConstructor(staticName = "create")
public class PgResultSet implements ResultSet {
    private final List<Row> rows;
    private final Map<String, PgColumn> columns;
    private final int updatedRows;

    @Override
    public Collection<String> getColumns() {
        return Optional
                .ofNullable(columns)
                .map(Map::keySet)
                .orElseGet(Collections::emptySet);
    }

    @Override
    public Iterator<Row> iterator() {
        return Optional
                .ofNullable(rows)
                .map(List::iterator)
                .orElseGet(Collections::emptyIterator);
    }

    @Override
    public Row row(int index) {
        return Optional
                .ofNullable(rows)
                .map(r -> r.get(index))
                .orElseThrow(IndexOutOfBoundsException::new);
    }

    @Override
    public int size() {
        return Optional
                .ofNullable(rows)
                .map(List::size)
                .orElse(0);
    }

    @Override
    public int updatedRows() {
        return updatedRows;
    }

}

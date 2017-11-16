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

import com.github.pgasync.Row;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.DataRow;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Result row, uses {@link DataConverter} for all conversions.
 *
 * @author Antti Laisi
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PgRow implements Row {
    DataRow data;
    DataConverter dataConverter;
    Map<String, PgColumn> columns;
    PgColumn[] pgColumns;

    @Override
    public String getString(int index) {
        return readValue(index, dataConverter::toString);
    }

    @Override
    public String getString(String column) {
        return readValue(column, dataConverter::toString);
    }

    @Override
    public Character getChar(int index) {
        return readValue(index, dataConverter::toChar);
    }

    @Override
    public Character getChar(String column) {
        return readValue(column, dataConverter::toChar);
    }

    @Override
    public Byte getByte(int index) {
        return readValue(index, dataConverter::toByte);
    }

    @Override
    public Byte getByte(String column) {
        return readValue(column, dataConverter::toByte);
    }

    @Override
    public Short getShort(int index) {
        return readValue(index, dataConverter::toShort);
    }

    @Override
    public Short getShort(String column) {
        return readValue(column, dataConverter::toShort);
    }

    @Override
    public Integer getInt(int index) {
        return readValue(index, dataConverter::toInteger);
    }

    @Override
    public Integer getInt(String column) {
        return readValue(column, dataConverter::toInteger);
    }

    @Override
    public Long getLong(int index) {
        return readValue(index, dataConverter::toLong);
    }

    @Override
    public Long getLong(String column) {
        return readValue(column, dataConverter::toLong);
    }

    @Override
    public BigInteger getBigInteger(int index) {
        return readValue(index, dataConverter::toBigInteger);
    }

    @Override
    public BigInteger getBigInteger(String column) {
        return readValue(column, dataConverter::toBigInteger);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return readValue(index, dataConverter::toBigDecimal);
    }

    @Override
    public BigDecimal getBigDecimal(String column) {
        return readValue(column, dataConverter::toBigDecimal);
    }

    @Override
    public Double getDouble(int index) {
        return readValue(index, dataConverter::toDouble);
    }

    @Override
    public Double getDouble(String column) {
        return readValue(column, dataConverter::toDouble);
    }

    @Override
    public Date getDate(int index) {
        return readValue(index, dataConverter::toDate);
    }

    @Override
    public Date getDate(String column) {
        return readValue(column, dataConverter::toDate);
    }

    @Override
    public Time getTime(int index) {
        return readValue(index, dataConverter::toTime);
    }

    @Override
    public Time getTime(String column) {
        return readValue(column, dataConverter::toTime);
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return readValue(index, dataConverter::toTimestamp);
    }

    @Override
    public Timestamp getTimestamp(String column) {
        return readValue(column, dataConverter::toTimestamp);
    }

    @Override
    public byte[] getBytes(int index) {
        return readValue(index, dataConverter::toBytes);
    }

    @Override
    public byte[] getBytes(String column) {
        return readValue(column, dataConverter::toBytes);
    }

    @Override
    public Boolean getBoolean(int index) {
        return readValue(index, dataConverter::toBoolean);
    }

    @Override
    public Boolean getBoolean(String column) {
        return readValue(column, dataConverter::toBoolean);
    }

    @Override
    public <TArray> TArray getArray(String column, Class<TArray> arrayType) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toArray(arrayType, pgColumn.type(), data.getValue(pgColumn.index()));
    }

    @Override
    public <TArray> TArray getArray(int index, Class<TArray> arrayType) {
        return dataConverter.toArray(arrayType, pgColumns[index].type(), data.getValue(index));
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        return dataConverter.toObject(type, pgColumns[index].type(), data.getValue(index));
    }

    @Override
    public <T> T get(String column, Class<T> type) {
        PgColumn pgColumn = getColumn(column);
        return dataConverter.toObject(type, pgColumn.type(), data.getValue(pgColumn.index()));
    }

    public Object get(String column) {
        return readValue(column, dataConverter::toObject);
    }

    private <T> T readValue(String column, BiFunction<Oid, byte[], T> converter) {
        PgColumn pgColumn = getColumn(column);
        return converter.apply(pgColumn.type(), data.getValue(pgColumn.index()));
    }

    private <T> T readValue(int index, BiFunction<Oid, byte[], T> converter) {
        if (index > pgColumns.length)
            throw new IndexOutOfBoundsException("No such column: " + index);

        return converter.apply(pgColumns[index].type(), data.getValue(index));
    }

    private PgColumn getColumn(String name) {
        if (name == null)
            throw new IllegalArgumentException("Column name is required");

        PgColumn column = columns.get(name.toUpperCase());
        if (column == null)
            throw new SqlException("Unknown column '" + name + "'");
        
        return column;
    }

    public static PgRow create(DataRow data, Map<String, PgColumn> columns, DataConverter dataConverter) {
        Collection<PgColumn> values = columns.values();
        return new PgRow(data, dataConverter, columns, values.toArray(new PgColumn[values.size()]));
    }
}

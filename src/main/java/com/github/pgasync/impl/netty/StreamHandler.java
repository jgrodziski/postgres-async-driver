package com.github.pgasync.impl.netty;

import com.github.pgasync.impl.PgColumn;
import com.github.pgasync.impl.message.CommandComplete;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.RowDescription;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class StreamHandler {
    private ChannelHandlerContext context;
    private Map<String, PgColumn> columns;
    private int updated;

    void init(ChannelHandlerContext context) {
        this.context = context;
    }

    public void rowDescription(RowDescription rowDescription) {
        this.columns = readColumns(rowDescription.columns());
    }

    public abstract void dataRow(DataRow dataRow);

    public void complete(CommandComplete commandComplete) {
        this.updated = commandComplete.updatedRows();
    }

    private Map<String, PgColumn> readColumns(RowDescription.ColumnDescription[] descriptions) {
        Map<String, PgColumn> columns = new HashMap<>();

        for (int i = 0; i < descriptions.length; i++) {
            String columnName = descriptions[i].name().toUpperCase();
            PgColumn pgColumn = PgColumn.create(i, descriptions[i].type());
            columns.put(columnName, pgColumn);
        }

        return columns;
    }

    public static StreamHandler ignoreData() {
        return new StreamHandler() {
            @Override
            public void dataRow(DataRow dataRow) {
            }
        };
    }
}

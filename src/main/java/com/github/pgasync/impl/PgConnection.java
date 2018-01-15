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

import com.github.pgasync.Connection;
import com.github.pgasync.ResultSet;
import com.github.pgasync.Row;
import com.github.pgasync.Transaction;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.*;
import com.github.pgasync.impl.protocol.ProtocolStream;
import lombok.Getter;
import lombok.experimental.Accessors;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.nurkiewicz.typeof.TypeOf.whenTypeOf;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single queryRows at a time.
 *
 * @author Antti Laisi
 * @author Jacek Sokol
 */
public class PgConnection implements Connection {
    private static int NEXT_CONNECTION_NUMBER = 0;
    private final int number;
    private final ProtocolStream stream;
    private final DataConverter dataConverter;
    private long timeout = 0;
    private Completable setTimeout = Completable.complete();

    PgConnection(ProtocolStream stream, DataConverter dataConverter) {
        this.number = NEXT_CONNECTION_NUMBER++;
        this.stream = stream;
        this.dataConverter = dataConverter;
    }

    @Override
    public Completable close() {
        return stream.close();
    }

    @Override
    public Observable<String> listen(String channel) {
        // TODO: wait for commit before sending unlisten as otherwise it can be rolled back
        AtomicBoolean stopped = new AtomicBoolean();
        Action0 ensureStopped = () -> {
            if (!stopped.get()) {
                stopped.set(true);
                querySet("UNLISTEN " + channel).subscribe();
            }
        };

        return querySet("LISTEN " + channel)
                .flatMapObservable(__ -> stream.listen(channel))
                .doOnUnsubscribe(ensureStopped)
                .doOnTerminate(ensureStopped);
    }

    @Override
    public Single<Transaction> begin() {
        return querySet("BEGIN").map(__ -> new PgConnectionTransaction());
    }

    @Override
    public Observable<Row> queryRows(String sql, Object... params) {
        ResultBuilder resultBuilder = new ResultBuilder(dataConverter);
        return sendCommand(sql, params)
                .lift(resultBuilder);
    }

    @Override
    public Single<ResultSet> querySet(String sql, Object... params) {
        ResultBuilder resultBuilder = new ResultBuilder(dataConverter);
        Func2<ArrayList<Row>, Row, ArrayList<Row>> reducer = (list, row) -> {
            list.add(row);
            return list;
        };

        return sendCommand(sql, params)
                .lift(resultBuilder)
                .reduce(new ArrayList<>(), reducer)
                .<ResultSet>map(rows -> PgResultSet.create(rows, resultBuilder.columns(), resultBuilder.updated()))
                .last()
                .toSingle();
    }

    @Override
    public Connection withTimeout(long value, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(value);
        if (millis != timeout) {
            timeout = millis;
            setTimeout = sendCommand("SET statement_timeout = " + millis, new Object[]{}).last().toCompletable();
        }

        return this;
    }

    @Override
    public boolean isConnected() {
        return stream.isConnected();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgConnection that = (PgConnection) o;

        return number == that.number;
    }

    @Override
    public int hashCode() {
        return number;
    }

    @Override
    public String toString() {
        return "PgConnection{" +
                "number=" + number +
                ", timeout=" + timeout +
                ", connected=" + stream.isConnected() +
                '}';
    }

    private Observable<Message> sendCommand(String sql, Object[] params) {
        Observable<Message> command = (params == null || params.length == 0)
                ? stream.command(new Query(sql))
                : stream.command(
                new Parse(sql),
                new Bind(dataConverter.fromParameters(params)),
                ExtendedQuery.DESCRIBE,
                ExtendedQuery.EXECUTE,
                ExtendedQuery.CLOSE,
                ExtendedQuery.SYNC
        );

        return setTimeout.doOnCompleted(() -> setTimeout = Completable.complete()).andThen(command);
    }

    Single<Connection> connect(String username, String password, String database) {
        return stream.connect(new StartupMessage(username, database))
                .flatMapCompletable(auth ->
                        auth.success()
                                ? Completable.complete()
                                : stream.authenticate(new PasswordMessage(username, password, auth.md5salt()))
                )
                .toSingleDefault(this);
    }

    @Getter
    @Accessors(fluent = true)
    static class ResultBuilder implements Observable.Operator<Row, Message> {
        private final DataConverter dataConverter;

        private Map<String, PgColumn> columns;
        private int updated;

        ResultBuilder(DataConverter dataConverter) {
            this.dataConverter = dataConverter;
        }

        @Override
        public Subscriber<? super Message> call(Subscriber<? super Row> subscriber) {
            return new Subscriber<Message>(subscriber) {
                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onNext(Message o) {
                    whenTypeOf(o)
                            .is(DataRow.class).then(dataRow -> subscriber.onNext(PgRow.create(dataRow, columns, dataConverter)))
                            .is(RowDescription.class).then(rowDescription -> columns = readColumns(rowDescription.columns()))
                            .is(CommandComplete.class).then(commandComplete -> updated = commandComplete.updatedRows());
                }
            };
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
    }

    /**
     * Transaction that rollbacks the tx on backend error and closes the connection on COMMIT/ROLLBACK failure.
     */
    class PgConnectionTransaction implements Transaction {
        @Override
        public Single<Transaction> begin() {
            return querySet("SAVEPOINT sp_1").map(rs -> new PgConnectionNestedTransaction(1));
        }

        @Override
        public Completable commit() {
            return PgConnection.this.querySet("COMMIT")
                    .toCompletable()
                    .onErrorResumeNext(this::closeStream);
        }

        @Override
        public Completable rollback() {
            return PgConnection.this.querySet("ROLLBACK")
                    .toCompletable()
                    .onErrorResumeNext(this::closeStream);
        }

        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            return PgConnection.this.queryRows(sql, params)
                    .onErrorResumeNext(this::doRollback);
        }

        @Override
        public Single<ResultSet> querySet(String sql, Object... params) {
            return PgConnection.this.querySet(sql, params)
                    .onErrorResumeNext(t -> this.<ResultSet>doRollback(t).toSingle());
        }

        @Override
        public Transaction withTimeout(long timeout, TimeUnit timeUnit) {
            PgConnection.this.withTimeout(timeout, timeUnit);
            return this;
        }

        private <T> Observable<T> doRollback(Throwable t) {
            return PgConnection.this.isConnected()
                    ? rollback().andThen(Observable.error(t))
                    : Observable.error(t);
        }

        private Completable closeStream(Throwable exception) {
            return stream.close().onErrorComplete().andThen(Completable.error(exception));
        }
    }

    /**
     * Nested Transaction using savepoints.
     */
    class PgConnectionNestedTransaction extends PgConnectionTransaction {
        final int depth;

        PgConnectionNestedTransaction(int depth) {
            this.depth = depth;
        }

        @Override
        public Single<Transaction> begin() {
            return querySet("SAVEPOINT sp_" + (depth + 1))
                    .map(rs -> new PgConnectionNestedTransaction(depth + 1));
        }

        @Override
        public Completable commit() {
            return PgConnection.this.querySet("RELEASE SAVEPOINT sp_" + depth)
                    .toCompletable();
        }

        @Override
        public Completable rollback() {
            return PgConnection.this.querySet("ROLLBACK TO SAVEPOINT sp_" + depth)
                    .toCompletable();
        }
    }
}

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
import com.github.pgasync.impl.netty.PgProtocolStream;
import com.github.pgasync.impl.netty.StreamHandler;
import rx.Completable;
import rx.Emitter;
import rx.Observable;
import rx.Single;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A connection to PostgreSQL backed. The postmaster forks a backend process for
 * each connection. A connection can process only a single queryRows at a time.
 *
 * @author Antti Laisi
 */
public class PgConnection implements Connection {
    private final PgProtocolStream stream;
    private final DataConverter dataConverter;
    private long timeout = 0;
    private Completable setTimeout = Completable.complete();

    PgConnection(PgProtocolStream stream, DataConverter dataConverter) {
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
        return querySet("LISTEN " + channel)
                .flatMapObservable(__ ->
                        Observable.<String>create(emitter ->
                                        emitter.setSubscription(
                                                stream.listen(emitter::onNext, channel)
                                                        .doOnCompleted(emitter::onCompleted)
                                                        .doOnError(emitter::onError)
                                                        .subscribe()
                                        ),
                                Emitter.BackpressureMode.LATEST)
                )
                .doOnUnsubscribe(() -> querySet("UNLISTEN " + channel).subscribe());
    }

    @Override
    public Single<Transaction> begin() {
        return querySet("BEGIN").map(__ -> new PgConnectionTransaction());
    }

    @Override
    public Observable<Row> queryRows(String sql, Object... params) {
        Consumer<BackpressuredEmitter> emitterAction = emitter ->
                emitter.setSubscription(
                        sendCommand(emitter, sql, params)
                                .doOnCompleted(emitter::onCompleted)
                                .doOnError(emitter::onError)
                                .onErrorComplete()
                                .subscribe()
                );

        return Observable.unsafeCreate(BackpressuredEmitter.create(emitterAction, dataConverter));
    }

    @Override
    public Single<ResultSet> querySet(String sql, Object... params) {
        ResultSetHandler handler = new ResultSetHandler();
        return sendCommand(handler, sql, params).toSingle(handler::resultSet);
    }

    @Override
    public Connection withTimeout(long value, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(value);
        if (millis != timeout) {
            timeout = millis;
            setTimeout = sendCommand(StreamHandler.ignoreData(), "SET statement_timeout = " + millis, new Object[]{});
        }

        return this;
    }

    @Override
    public boolean isConnected() {
        return stream.isConnected();
    }

    private Completable sendCommand(StreamHandler handler, String sql, Object[] params) {
        Completable command = (params == null || params.length == 0)
                ? stream.command(handler, new Query(sql))
                : stream.command(
                handler,
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

    class ResultSetHandler extends StreamHandler {
        private List<Row> rows = new ArrayList<>();

        ResultSet resultSet() {
            return PgResultSet.create(rows, columns(), updated());
        }

        @Override
        public void dataRow(DataRow dataRow) {
            rows.add(PgRow.create(dataRow, columns(), dataConverter));
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

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

import com.github.pgasync.*;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.netty.PgProtocolStream;
import io.netty.channel.EventLoopGroup;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.SingleSubscriber;
import rx.functions.Action0;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pool for backend connections. Callbacks are queued and executed when pool has an available
 * connection.
 *
 * @author Antti Laisi
 */
public class PgConnectionPool implements ConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(PgConnectionPool.class);

    private final Queue<SingleSubscriber<Connection>> subscribers = new LinkedList<>();
    private final Set<Connection> connections = new HashSet<>();
    private final Queue<Connection> availableConnections = new LinkedList<>();
    private final ConnectionPool delegate = new ConnectionPoolWithTimeout(0);

    private final ConnectionConfig config;
    private final DataConverter dataConverter;
    private final EventLoopGroup eventLoopGroup;
    private final NettyScheduler scheduler;

    private int currentSize;
    private volatile boolean closed;

    public PgConnectionPool(ConnectionConfig config, DataConverter dataConverter, EventLoopGroup eventLoopGroup) {
        this.config = config;
        this.dataConverter = dataConverter;
        this.eventLoopGroup = eventLoopGroup;
        this.scheduler = NettyScheduler.forEventExecutor(eventLoopGroup.next());
    }

    @Override
    public Observable<Row> queryRows(String sql, Object... params) {
        return delegate.queryRows(sql, params);
    }

    @Override
    public Single<ResultSet> querySet(String sql, Object... params) {
        return delegate.querySet(sql, params);
    }

    @Override
    public Single<Transaction> begin() {
        return delegate.begin();
    }

    @Override
    public Observable<String> listen(String channel) {
        return delegate.listen(channel);
    }

    @Override
    public ConnectionPool withTimeout(long timeout, TimeUnit timeUnit) {
        return new ConnectionPoolWithTimeout(timeUnit.toMillis(timeout));
    }

    @Override
    public Completable close() {
        if (closed)
            return Completable.complete();

        closed = true;
        revokeSubscribers();

        return waitForConnectionsToBeReleased()
                .andThen(closeEventLoop())
                .doOnCompleted(() -> LOG.info("Connection pool closed"));
    }

    private Completable closeEventLoop() {
        return Completable.create(subscriber -> {
            LOG.debug("Closing event loop");
            eventLoopGroup
                    .shutdownGracefully()
                    .addListener(f -> {
                        if (f.isSuccess())
                            subscriber.onCompleted();
                        else
                            subscriber.onError(f.cause());
                    });
        });
    }

    private Completable waitForConnectionsToBeReleased() {
        AtomicBoolean done = new AtomicBoolean();
        return Observable
                .interval(100, 1000, MILLISECONDS)
                .doOnSubscribe(() -> LOG.debug("Waiting for connections to be released: {}", connections.size()))
                .doOnNext(__ -> {
                    while (currentSize > 0) {
                        Connection connection = availableConnections.poll();
                        if (connection != null) {
                            currentSize--;
                            connection.close();
                        } else {
                            break;
                        }
                    }
                    done.set(currentSize == 0);
                })
                .takeWhile(__ -> !done.get())
                .toCompletable();
    }

    private void revokeSubscribers() {
        LOG.debug("Revoking subscribers: {}", subscribers.size());
        subscribers.forEach(subscriber -> subscriber.onError(new SqlException("Connection pool is closing")));
        subscribers.clear();
    }

    @Override
    public Single<Connection> getConnection() {
        return Single
                .<Connection>create(this::subscribeForConnection)
                .subscribeOn(scheduler);
    }

    @Override
    public Completable release(Connection connection) {
        return Completable
                .create(subscriber -> {
                    if (connections.contains(connection) && ! availableConnections.contains(connection))
                        availableConnections.add(connection);
                    managePool();
                    subscriber.onCompleted();
                })
                .subscribeOn(scheduler);
    }

    @SuppressWarnings("unchecked")
    private void subscribeForConnection(SingleSubscriber<? super Connection> subscriber) {
        if (closed)
            subscriber.onError(new SqlException("Connection pool is closed"));
        else {
            subscribers.add((SingleSubscriber<Connection>) subscriber);
            managePool();
        }
    }

    private void openConnectionsIfNecessary() {
        if (currentSize >= config.poolSize() || subscribers.size() <= connections.size() || closed)
            return;

        int connectionsToOpen = Math.min(subscribers.size(), config.poolSize() - currentSize);
        currentSize += connectionsToOpen;

        IntStream.range(0, connectionsToOpen)
                .forEach(__ ->
                        new PgConnection(new PgProtocolStream(eventLoopGroup, config), dataConverter)
                                .connect(config.username(), config.password(), config.database())
                                .doOnEach(___ -> houseKeepSubscribers())
                                .doOnSuccess(connection -> {
                                    LOG.info("New connection created [{}/{}]", currentSize, config.poolSize());
                                    connections.add(connection);
                                    availableConnections.add(connection);
                                    serveAvailableConnections();
                                })
                                .doOnError(exception -> {
                                    LOG.debug("Failed to create connection", exception);
                                    currentSize--;
                                    Optional.ofNullable(subscribers.remove())
                                            .ifPresent(s -> s.onError(exception));
                                    openConnectionsIfNecessary();
                                })
                                .subscribe()
                );
    }

    private void managePool() {
        houseKeepSubscribers();
        houseKeepConnections();
        openConnectionsIfNecessary();
        serveAvailableConnections();
    }

    private void serveAvailableConnections() {
        while (!subscribers.isEmpty() && !availableConnections.isEmpty() && !closed)
            subscribers.poll().onSuccess(availableConnections.poll());
    }

    private void houseKeepConnections() {
        Map<Boolean, List<Connection>> connectionsByStatus = availableConnections.stream().collect(Collectors.partitioningBy(Connection::isConnected));
        List<Connection> dirtyConnections = connectionsByStatus.get(false);
        dirtyConnections.forEach(this::closeConnectionQuietly);
        connections.removeAll(dirtyConnections);
        availableConnections.removeAll(dirtyConnections);
    }

    private void houseKeepSubscribers() {
        while (!subscribers.isEmpty() && subscribers.peek().isUnsubscribed())
            subscribers.remove();
    }

    private void closeConnectionQuietly(Connection connection) {
        LOG.info("Removing dirty connection [{}/{}]", currentSize, config.poolSize());
        currentSize--;
        try {
            connection.close();
        } catch (Exception e) {
            LOG.debug("Error occurred while closing connection", e);
        }
    }

    /**
     * Transaction that chains releasing the connection after COMMIT/ROLLBACK.
     */
    class ReleasingTransaction implements Transaction {
        final AtomicBoolean released = new AtomicBoolean();
        final Connection txConnection;
        final Transaction transaction;

        ReleasingTransaction(Connection txConnection, Transaction transaction) {
            this.txConnection = txConnection;
            this.transaction = transaction;
        }

        @Override
        public Single<Transaction> begin() {
            // Nested transactions should not release things automatically.
            return transaction.begin();
        }

        @Override
        public Completable rollback() {
            return transaction
                    .rollback()
                    .doOnTerminate(this::releaseConnectionImmediately);
        }

        @Override
        public Completable commit() {
            return transaction
                    .commit()
                    .doOnTerminate(this::releaseConnectionImmediately);
        }

        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            if (released.get()) {
                return Observable.error(new SqlException("Transaction is already completed"));
            }

            AtomicBoolean completed = new AtomicBoolean();

            return transaction
                    .queryRows(sql, params)
                    .onErrorResumeNext(exception -> releaseConnection().andThen(Observable.error(exception)))
                    .doOnUnsubscribe(() -> {
                        if (!completed.get())
                            releaseConnectionImmediately();
                    });
        }

        @Override
        public Single<ResultSet> querySet(String sql, Object... params) {
            if (released.get()) {
                return Single.error(new SqlException("Transaction is already completed"));
            }

            AtomicBoolean completed = new AtomicBoolean();

            return transaction
                    .querySet(sql, params)
                    .doOnSuccess(__ -> completed.set(true))
                    .onErrorResumeNext(exception -> releaseConnection().andThen(Single.error(exception)))
                    .doOnUnsubscribe(() -> {
                        if (!completed.get())
                            releaseConnectionImmediately();
                    });
        }

        @Override
        public Transaction withTimeout(long timeout, TimeUnit timeUnit) {
            return transaction.withTimeout(timeout, timeUnit);
        }

        Completable releaseConnection() {
            return released.get()
                    ? Completable.complete()
                    : release(txConnection).doOnCompleted(() -> released.set(true));
        }

        void releaseConnectionImmediately() {
            releaseConnection().subscribe();
        }
    }

    @RequiredArgsConstructor
    class ConnectionPoolWithTimeout implements ConnectionPool {
        private final long timeout;

        @RequiredArgsConstructor
        class ReleaseEnforcer implements Action0 {
            final Connection connection;
            volatile boolean released;

            @Override
            public void call() {
                if (!released) {
                    released = true;
                    releaseIfNotPipelining(connection);
                }
            }
        }

        @Override
        public Single<Connection> getConnection() {
            return PgConnectionPool.this.getConnection()
                    .map(connection -> connection.withTimeout(timeout, MILLISECONDS));
        }

        @Override
        public Single<Transaction> begin() {
            return getConnection()
                    .flatMap(connection -> connection
                            .begin()
                            .onErrorResumeNext(t -> release(connection).andThen(Single.error(t)))
                            .map(tx -> new ReleasingTransaction(connection, tx))
                    );
        }

        @Override
        public Observable<Row> queryRows(String sql, Object... params) {
            return getConnection()
                    .doOnSuccess(this::releaseIfPipelining)
                    .flatMapObservable(connection -> {
                        ReleaseEnforcer releaseEnforcer = new ReleaseEnforcer(connection);
                        return connection
                                .queryRows(sql, params)
                                .doOnTerminate(releaseEnforcer)
                                .doOnUnsubscribe(releaseEnforcer);
                    });
        }

        @Override
        public Single<ResultSet> querySet(String sql, Object... params) {
            return getConnection()
                    .doOnSuccess(this::releaseIfPipelining)
                    .flatMap(connection -> {
                        ReleaseEnforcer releaseEnforcer = new ReleaseEnforcer(connection);
                        return connection
                                .querySet(sql, params)
                                .doAfterTerminate(releaseEnforcer)
                                .doOnUnsubscribe(releaseEnforcer);
                    });
        }

        @Override
        public Observable<String> listen(String channel) {
            return getConnection()
                    .flatMapObservable(connection ->
                            connection
                                    .listen(channel)
                                    .doOnSubscribe(() -> release(connection).subscribe())
                    );
        }

        @Override
        public ConnectionPool withTimeout(long timeout, TimeUnit timeUnit) {
            return PgConnectionPool.this.withTimeout(timeout, timeUnit);
        }

        @Override
        public Completable release(Connection connection) {
            return PgConnectionPool.this.release(connection.withTimeout(0, SECONDS));
        }

        @Override
        public Completable close() {
            return PgConnectionPool.this.close();
        }

        private void releaseIfPipelining(Connection connection) {
            if (config.pipeline())
                release(connection).subscribe();
        }

        private void releaseIfNotPipelining(Connection connection) {
            if (!config.pipeline())
                release(connection).subscribe();
        }
    }
}

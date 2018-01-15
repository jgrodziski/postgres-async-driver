package com.github.pgasync.impl;

import com.github.pgasync.*;
import org.junit.rules.ExternalResource;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.System.getenv;
import static java.lang.System.out;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.V9_6_2;

/**
 * @author Antti Laisi
 */
class DatabaseRule extends ExternalResource {
    interface CheckedConsumer<T> {
        void accept(T t) throws Exception;
    }

    private static PostgresProcess process;
    final ConnectionPoolBuilder builder;
    ConnectionPool pool;

    DatabaseRule() {
        this(createPoolBuilder(1));
    }

    DatabaseRule(ConnectionPoolBuilder builder) {
        this.builder = builder;
        if (builder instanceof EmbeddedConnectionPoolBuilder) {
            if (process == null) {
                try {
                    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
                    PostgresConfig config = new PostgresConfig(V9_6_2, new AbstractPostgresConfig.Net(),
                            new AbstractPostgresConfig.Storage("async-pg"), new AbstractPostgresConfig.Timeout(),
                            new AbstractPostgresConfig.Credentials("async-pg", "async-pg"));
                    PostgresExecutable exec = runtime.prepare(config);
                    process = exec.start();

                    out.printf("Started postgres to %s:%d%n", process.getConfig().net().host(), process.getConfig().net().port());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            builder.hostname(process.getConfig().net().host());
            builder.port(process.getConfig().net().port());
        }
    }

    @Override
    protected void before() {
        pool = Optional
                .ofNullable(pool)
                .orElseGet(builder::build);
    }

    @Override
    protected void after() {
        Optional.ofNullable(pool)
                .map(Db::close)
                .ifPresent(Completable::await);
    }

    ResultSet query(String sql) {
        return block(db().querySet(sql, (Object[]) null).toObservable());
    }

    @SuppressWarnings("rawtypes")
    ResultSet query(String sql, List/*<Object>*/ params) {
        return block(db().querySet(sql, params.toArray()).toObservable());
    }

    void withConnection(CheckedConsumer<Connection> action) {
        pool.getConnection()
                .observeOn(Schedulers.computation())
                .flatMapCompletable(c -> {
                    try {
                        action.accept(c);
                        return Completable.complete();
                    } catch (Throwable t) {
                        return Completable.error(t);
                    } finally {
                        pool.release(c).await();
                    }
                })
                .await();
    }

    private <T> T block(Observable<T> observable) {
        BlockingQueue<Entry<T, Throwable>> result = new ArrayBlockingQueue<>(1);
        observable.single().subscribe(
                item -> result.add(new SimpleImmutableEntry<>(item, null)),
                exception -> result.add(new SimpleImmutableEntry<>(null, exception)));
        try {

            Entry<T, Throwable> entry = result.poll(5, TimeUnit.SECONDS);
            if (entry == null) {
                throw new RuntimeException("Timed out waiting for result");
            }
            Throwable exception = entry.getValue();
            if (exception != null) {
                throw exception instanceof RuntimeException
                        ? (RuntimeException) exception
                        : new RuntimeException(exception);
            }
            return entry.getKey();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    Db db() {
        before();
        return pool;
    }

    static class EmbeddedConnectionPoolBuilder extends ConnectionPoolBuilder {
        EmbeddedConnectionPoolBuilder() {
            database("async-pg");
            username("async-pg");
            password("async-pg");
        }
    }

    static ConnectionPoolBuilder createPoolBuilder(int size) {
        String db = getenv("PG_DATABASE");
        String user = getenv("PG_USERNAME");
        String pass = getenv("PG_PASSWORD");

        ConnectionPoolBuilder connectionPoolBuilder = new EmbeddedConnectionPoolBuilder()
                .connectTimeout(1, TimeUnit.SECONDS)
                .poolSize(size);

        if (db == null && user == null && pass == null)
            return connectionPoolBuilder;
        else
            return connectionPoolBuilder
                    .database(envOrDefault("PG_DATABASE"))
                    .username(envOrDefault("PG_USERNAME"))
                    .password(envOrDefault("PG_PASSWORD"))
                    .ssl(true);
    }


    private static String envOrDefault(String var) {
        String value = getenv(var);
        return value != null ? value : "postgres";
    }
}

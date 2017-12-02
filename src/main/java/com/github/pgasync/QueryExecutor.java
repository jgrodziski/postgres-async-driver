package com.github.pgasync;

import rx.Observable;
import rx.Single;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * QueryExecutor submits SQL for execution.
 *
 * @author Antti Laisi
 */
public interface QueryExecutor {
    /**
     * Begins a transaction.
     *
     * @return Cold single that returns started transaction.
     */
    Single<Transaction> begin();

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     * Supports back-pressure in the returned stream.
     *
     * @param sql    SQL to execute
     * @param params Parameter values
     * @return Cold observable that emits 0-n rows.
     */
    Observable<Row> queryRows(String sql, Object... params);

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     * Doesn't support back-pressure.
     *
     * @param sql    SQL to execute
     * @param params Parameter values
     * @return Cold single that emits a single result set.
     */
    Single<ResultSet> querySet(String sql, Object... params);

    /**
     * Sets statement timeout on the executor and returns it.
     * @param timeout  timeout value
     * @param timeUnit time unit
     * @return returns query executor with timeout set
     */
    QueryExecutor withTimeout(long timeout, TimeUnit timeUnit);

    /**
     * Begins a transaction.
     *
     * @param onTransaction Called when transaction is successfully started.
     * @param onError       Called on exception thrown
     */
    default void begin(Consumer<Transaction> onTransaction, Consumer<Throwable> onError) {
        begin().subscribe(onTransaction::accept, onError::accept);
    }

    /**
     * Executes a simple query.
     *
     * @param sql      SQL to execute.
     * @param onResult Called when query is completed
     * @param onError  Called on exception thrown
     */
    default void query(String sql, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        query(sql, Collections.emptyList(), onResult, onError);
    }

    /**
     * Executes an anonymous prepared statement. Uses native PostgreSQL syntax with $arg instead of ?
     * to mark parameters. Supported parameter types are String, Character, Number, Time, Date, Timestamp
     * and byte[].
     *
     * @param sql      SQL to execute
     * @param params   List of parameters
     * @param onResult Called when query is completed
     * @param onError  Called on exception thrown
     */
    default void query(String sql, List<?> params, Consumer<ResultSet> onResult, Consumer<Throwable> onError) {
        querySet(sql, params.toArray()).subscribe(onResult::accept, onError::accept);
    }
}

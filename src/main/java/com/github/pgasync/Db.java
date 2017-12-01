package com.github.pgasync;

import rx.Completable;

/**
 * Main interface to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor, Listenable {
    /**
     * Closes the pool. Under the hood it waits for all connections to be released.
     */
    Completable close();
}

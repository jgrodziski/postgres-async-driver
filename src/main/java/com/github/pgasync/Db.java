package com.github.pgasync;

import rx.Completable;

/**
 * Main interface to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public interface Db extends QueryExecutor, Listenable {
    /**
     * Closes the pool, blocks the calling thread until connections are closed.
     */
    Completable close();
}

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

import com.github.pgasync.ConnectionPool;
import com.github.pgasync.ResultSet;
import com.github.pgasync.SqlException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.Single;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * Tests for connection pool concurrency.
 *
 * @author Antti Laisi
 */
public class ConnectionPoolingTest {

    @Rule
    public final DatabaseRule dbr = new DatabaseRule();

    @Before
    public void create() {
        dbr.query("DROP TABLE IF EXISTS CP_TEST; CREATE TABLE CP_TEST (ID VARCHAR(255) PRIMARY KEY)");
    }

    @After
    public void drop() {
        dbr.query("DROP TABLE CP_TEST");
    }

    @Test
    public void shouldRunAllQueuedCallbacks() throws Exception {
        final int count = 1000;
        IntFunction<Callable<ResultSet>> insert = value -> () -> dbr.query("INSERT INTO CP_TEST VALUES($1)", singletonList(value));
        List<Callable<ResultSet>> tasks = IntStream.range(0, count).mapToObj(insert).collect(toList());

        ExecutorService executor = Executors.newFixedThreadPool(20);
        executor.invokeAll(tasks).forEach(this::await);

        assertEquals(count, dbr.query("SELECT COUNT(*) FROM CP_TEST").row(0).getLong(0).longValue());
    }

    @Test
    public void shouldResetTimeoutToDefaultValueWhenConnectionIsReturnedToPool() throws Exception {
        ConnectionPool pool = dbr.builder.statementTimeout(5, TimeUnit.SECONDS).poolSize(1).build();

        long timeSpent = testTimeout(
                pool.withTimeout(1, TimeUnit.SECONDS).querySet("SELECT pg_sleep(10)")
        );

        assertThat(timeSpent, is(both(greaterThanOrEqualTo(1000L)).and(lessThan(2000L))));

        timeSpent = testTimeout(
                pool.querySet("SELECT pg_sleep(10)")
        );

        assertThat(timeSpent, is(both(greaterThanOrEqualTo(5000L)).and(lessThan(6000L))));
    }

    @Test
    public void shouldForciblyClosePool() throws Exception {
        ConnectionPool pool = dbr.builder.poolSize(1).poolCloseTimeout(500, TimeUnit.MILLISECONDS).build();

        pool.getConnection().toBlocking().value();

        boolean closedCorrectly = pool.close().await(5, TimeUnit.SECONDS);

        assertTrue(closedCorrectly);
    }

    private long testTimeout(Single<ResultSet> query) {
        long time = System.currentTimeMillis();
        try {
            query.toCompletable().await();
            fail("TimeoutException expected but no exception thrown");
        } catch (RuntimeException e) {
            assertEquals(SqlException.class, e.getClass());
            assertThat(e.getMessage(), containsString("canceling statement due to statement timeout"));
        } finally {
            time = System.currentTimeMillis() - time;
        }

        return time;
    }

    private <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

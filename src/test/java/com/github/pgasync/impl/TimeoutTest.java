package com.github.pgasync.impl;

import com.github.pgasync.*;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import rx.Observable;
import rx.observables.BlockingObservable;
import rx.observers.TestSubscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TimeoutTest {
    private TestSubscriber testSubscriber;

    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Before
    public void setup() {
        testSubscriber = TestSubscriber.create();
    }

    @Test
    public void shouldReportErrorOnReadTimeout() throws Exception {
        //given
        Db db = dbr.db();

        //when
        db.querySet("select pg_sleep(10)").timeout(1, TimeUnit.SECONDS).subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        //then
        Object error = testSubscriber.getOnErrorEvents().get(0);
        assertThat(error, instanceOf(TimeoutException.class));
    }

    @Test
    public void shouldReportErrorWhenAttemptToNotExistingEndpoint() throws Exception {
        //given:
        int port = randomPort();
        ConnectionPool db = new ConnectionPoolBuilder().hostname("localhost")
                .port(port)
                .connectTimeout(1000)
                .build();

        //when
        db.querySet("select pg_sleep(5)").subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        //then
        Throwable actual = (Throwable) testSubscriber.getOnErrorEvents().get(0);
        assertEquals("Connection refused: localhost/127.0.0.1:" + port, actual.getMessage());
    }

    @Test
    public void shouldReportErrorWhenAttemptToConnectToNotRespondingEndPoint() throws Exception {
        //given:
        int port = randomPort();
        try (Socket socket = createDummySocket(port)) {
            ConnectionPool db = new ConnectionPoolBuilder()
                    .hostname("localhost")
                    .port(port)
                    .connectTimeout(1000)
                    .build();
            //when
            db.querySet("select pg_sleep(5)").subscribe(testSubscriber);
            testSubscriber.awaitTerminalEvent();

            //then
            Throwable actual = (Throwable) testSubscriber.getOnErrorEvents().get(0);
            assertEquals("connection timed out: localhost/127.0.0.1:" + port, actual.getMessage());
        }
    }

    @Test
    public void shouldReconnectAfterFailure() throws Exception {
        //given
        Db db = dbr.db();

        //when
        Observable
                .range(0, 5)
                .map(i -> (i + 1) % 4)
                .flatMap(i ->
                                db.queryRows("SELECT pg_sleep(" + i + ")")
                                        .timeout(1800, TimeUnit.MILLISECONDS)
                                        .map(x -> "ok")
                                        .onErrorReturn(e -> "error")
                        , 1)
                .subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();

        //then
        testSubscriber.assertValues("ok", "error", "error", "ok", "ok");
    }

    @Test
    public void shouldReconnectAfterTransactionTimeout() throws Exception {
        // given
        Db pool1 = dbr.builder.poolSize(1).build();
        Db pool2 = dbr.builder.poolSize(1).build();

        dbr.query("DROP TABLE IF EXISTS tx_timeout_test");
        dbr.query("CREATE TABLE tx_timeout_test(ID INT PRIMARY KEY)");


        Function<Integer, BlockingObservable<Void>> insertRecord = n -> pool2
                .begin()
                .flatMap(t ->
                        t.querySet("INSERT INTO tx_timeout_test values ($1)", n)
                                .map(__ -> t)
                                .flatMap(Transaction::commit)
                                .onErrorResumeNext(e -> t.rollback().flatMap(__ -> Observable.error(e)))
                                .timeout(1, TimeUnit.SECONDS)
                )
                .toBlocking();

        //when

        // lock table
        Transaction tx = pool1
                .begin()
                .flatMap(_tx -> _tx.querySet("LOCK TABLE tx_timeout_test IN ACCESS EXCLUSIVE MODE").map(__ -> _tx))
                .toBlocking()
                .last();

        // try to use that table in other tx
        try {
            insertRecord.apply(321).subscribe();
        } catch (Throwable t) {
            // ensure timeout is the cause
            assertThat(t.getCause(), is(instanceOf(TimeoutException.class)));
        } finally {
            // release lock
            tx.rollback().toBlocking().subscribe();
        }

        // try to insert once again
        insertRecord.apply(123).subscribe();

        //then
        List<Integer> records = pool2.queryRows("SELECT * FROM tx_timeout_test").map(r -> r.getInt(0)).toList().toBlocking().last();
        assertEquals(records, Collections.singletonList(123));
    }

    private Socket createDummySocket(int port) throws IOException {
        Socket socket = new Socket();
        socket.bind(new InetSocketAddress("localhost", port));
        return socket;
    }

    private int randomPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}

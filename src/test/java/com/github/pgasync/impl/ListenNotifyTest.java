package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPool;
import org.junit.ClassRule;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * @author Antti Laisi
 */
public class ListenNotifyTest {
    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(DatabaseRule.createPoolBuilder(5));

    @Test
    public void shouldReceiveNotificationsOnListenedChannel() throws Exception {
        ConnectionPool pool = dbr.pool;
        BlockingQueue<String> result = new LinkedBlockingQueue<>(5);

        Subscription subscription = pool.listen("example").subscribe(result::add, Throwable::printStackTrace);
        TimeUnit.SECONDS.sleep(2);

        pool.querySet("notify example, 'msg'").toBlocking().value();
        pool.querySet("notify example, 'msg'").toBlocking().value();
        pool.querySet("notify example, 'msg'").toBlocking().value();

        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));
        assertEquals("msg", result.poll(2, TimeUnit.SECONDS));

        subscription.unsubscribe();
        assertTrue(subscription.isUnsubscribed());

        pool.querySet("notify example, 'msg'").toBlocking().value();
        assertNull(result.poll(2, TimeUnit.SECONDS));
    }

    @Test
    public void shouldRespectBackPressureWhileListening() {
        final int N = 500;
        List<String> collectedNotifications = new LinkedList<>();
        CountDownLatch latch = new CountDownLatch(N);

        dbr.withConnection(connection -> {
            Subscription subscription = connection
                    .listen("test")
                    .onBackpressureLatest()
                    .observeOn(Schedulers.newThread())
                    .map(s -> {
                        try {
                            Thread.sleep(10);
                            collectedNotifications.add(s);
                            latch.countDown();
                        } catch (InterruptedException ignored) {
                        }
                        return s;
                    })
                    .subscribe();

            Observable.range(1, N)
                    .concatMap(n -> connection.querySet("notify test, '" + n + "'").toObservable())
                    .toCompletable()
                    .await();

            latch.await(10, TimeUnit.SECONDS);

            subscription.unsubscribe();

            assertThat(collectedNotifications.size(), lessThan(N));
            assertThat(collectedNotifications.get(collectedNotifications.size() - 1), is(String.valueOf(N)));
        });
    }
}

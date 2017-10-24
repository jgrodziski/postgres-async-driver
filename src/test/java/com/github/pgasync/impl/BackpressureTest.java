package com.github.pgasync.impl;

import org.junit.ClassRule;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.pgasync.impl.DatabaseRule.createPoolBuilder;
import static java.util.stream.IntStream.range;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BackpressureTest {
    @ClassRule
    public static DatabaseRule dbr = new DatabaseRule(createPoolBuilder(1));

    @Test
    public void shouldRespectBackpressureOnSlowConsumer() {
        //given
        List<Integer> numbers = range(0, 1000).boxed().collect(Collectors.toList());

        dbr.query("CREATE TABLE BACKPRESSURE_TEST(ID INT)");
        Observable.merge(
                shuffle(numbers)
                        .stream()
                        .map(__ -> dbr.db().querySet("INSERT INTO BACKPRESSURE_TEST VALUES($1)", __))
                        .collect(Collectors.toList())
        ).toCompletable().await(10, TimeUnit.SECONDS);

        // when
        List<Integer> result = dbr.db()
                .queryRows("SELECT * FROM BACKPRESSURE_TEST ORDER BY ID")
                .observeOn(Schedulers.computation())
                .doOnNext(__ -> sleep())
                .map(__ -> __.getInt(0))
                .toList()
                .sorted()
                .toBlocking()
                .last();

        // then
        assertThat(result, is(numbers));
    }

    private ArrayList<Integer> shuffle(List<Integer> numbers) {
        ArrayList<Integer> list = new ArrayList<>(numbers);
        Collections.shuffle(list);
        return list;
    }

    private void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

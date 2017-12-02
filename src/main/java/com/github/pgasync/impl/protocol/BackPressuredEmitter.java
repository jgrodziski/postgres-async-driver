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

package com.github.pgasync.impl.protocol;

import rx.Emitter;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Cancellable;
import rx.internal.operators.BackpressureUtils;
import rx.internal.subscriptions.CancellableSubscription;
import rx.subscriptions.SerialSubscription;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Emitter that supports back-pressure via injectable method.
 *
 * @param <T> type of elements in the stream
 * @author Jacek Sokol
 */
public class BackPressuredEmitter<T> implements Emitter<T>, Subscription, Producer {
    private static final int BUFFER_SIZE = 256;

    private final SerialSubscription serial = new SerialSubscription();
    private final AtomicLong requested = new AtomicLong();
    private final AtomicInteger wip = new AtomicInteger();
    private final BlockingDeque<T> buffer = new LinkedBlockingDeque<>(BUFFER_SIZE);

    private final Subscriber<T> subscriber;
    private final Runnable readNext;

    private BackPressuredEmitter(Subscriber<T> subscriber, Runnable readNext) {
        this.subscriber = subscriber;
        this.readNext = readNext;
    }

    @Override
    public void setSubscription(Subscription s) {
        serial.set(s);
    }

    @Override
    public void setCancellation(Cancellable c) {
        setSubscription(new CancellableSubscription(c));
    }

    @Override
    public long requested() {
        return requested.get();
    }

    @Override
    public void onCompleted() {
        if (subscriber.isUnsubscribed())
            return;

        try {
            subscriber.onCompleted();
        } finally {
            serial.unsubscribe();
        }
    }

    @Override
    public void onError(Throwable e) {
        if (subscriber.isUnsubscribed())
            return;

        try {
            subscriber.onError(e);
        } finally {
            serial.unsubscribe();
        }
    }

    @Override
    public void onNext(T t) {
        buffer.addFirst(t);
        drain();
    }

    @Override
    public void request(long n) {
        BackpressureUtils.getAndAddRequest(requested, n);
        drain();
    }

    @Override
    public void unsubscribe() {
        serial.unsubscribe();
        buffer.clear();
    }

    @Override
    public boolean isUnsubscribed() {
        return subscriber.isUnsubscribed();
    }

    private void drain() {
        if (wip.getAndIncrement() != 0)
            return;

        int missed = 1;
        for (; ; ) {
            if (subscriber.isUnsubscribed())
                break;

            int emitted = processQueue();
            BackpressureUtils.produced(requested, emitted);

            if (requested.get() - buffer.size() > 0)
                readNext.run();

            missed = wip.addAndGet(-missed);
            if (missed == 0)
                return;
        }
    }

    private int processQueue() {
        int emitted = 0;

        while (requested.get() > 0 && buffer.size() > 0) {
            T row = buffer.pollLast();
            subscriber.onNext(row);
            emitted++;
        }

        return emitted;
    }

    @SuppressWarnings("unchecked")
    static <R> OnSubscribe<R> create(Consumer<Emitter<R>> emitter, Runnable readNext) {
        return subscriber -> {
            BackPressuredEmitter<R> backPressuredEmitter = new BackPressuredEmitter(subscriber, readNext);
            subscriber.add(backPressuredEmitter);
            subscriber.setProducer(backPressuredEmitter);
            emitter.accept(backPressuredEmitter);
        };
    }
}

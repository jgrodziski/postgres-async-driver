package com.github.pgasync.impl;

import com.github.pgasync.Row;
import com.github.pgasync.impl.conversion.DataConverter;
import com.github.pgasync.impl.message.CommandComplete;
import com.github.pgasync.impl.message.DataRow;
import com.github.pgasync.impl.message.RowDescription;
import com.github.pgasync.impl.netty.StreamHandler;
import rx.*;
import rx.functions.Cancellable;
import rx.internal.operators.BackpressureUtils;
import rx.internal.subscriptions.CancellableSubscription;
import rx.subscriptions.SerialSubscription;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

class BackpressuredEmitter extends StreamHandler implements Emitter<Row>, Subscription, Producer {
    private static final int BUFFER_SIZE = 256;

    private final SerialSubscription serial = new SerialSubscription();
    private final BlockingDeque<Row> buffer = new LinkedBlockingDeque<>(BUFFER_SIZE);
    private final AtomicLong requested = new AtomicLong();
    private final AtomicInteger wip = new AtomicInteger();

    private final Subscriber<Row> subscriber;
    private final DataConverter dataConverter;

    private BackpressuredEmitter(Subscriber<Row> subscriber, DataConverter dataConverter) {
        this.subscriber = subscriber;
        this.dataConverter = dataConverter;
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
            readNext();
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
            enableAutoRead();
            subscriber.onError(e);
        } finally {
            serial.unsubscribe();
        }
    }

    @Override
    public void onNext(Row t) {
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
        if (context() == null)
            return;

        if (wip.getAndIncrement() != 0)
            return;

        int missed = 1;
        for (; ; ) {
            if (subscriber.isUnsubscribed())
                break;

            int emitted = processQueue();
            BackpressureUtils.produced(requested, emitted);

            if (requested.get() - buffer.size() > 0)
                readNext();

            missed = wip.addAndGet(-missed);
            if (missed == 0)
                return;
        }
    }

    private int processQueue() {
        int emitted = 0;

        while (requested.get() > 0 && buffer.size() > 0) {
            Row row = buffer.pollLast();

            subscriber.onNext(row);
            emitted++;
        }

        return emitted;
    }

    @SuppressWarnings("unchecked")
    static Observable.OnSubscribe<Row> create(Consumer<BackpressuredEmitter> emitter, DataConverter dataConverter) {
        return subscriber -> {
            BackpressuredEmitter extEmitter = new BackpressuredEmitter((Subscriber<Row>) subscriber, dataConverter);
            subscriber.add(extEmitter);
            subscriber.setProducer(extEmitter);
            emitter.accept(extEmitter);
        };
    }

    @Override
    public void rowDescription(RowDescription rowDescription) {
        super.rowDescription(rowDescription);
        disableAutoRead();
        drain();
    }

    @Override
    public void complete(CommandComplete commandComplete) {
        enableAutoRead();
        super.complete(commandComplete);
    }

    @Override
    public void dataRow(DataRow dataRow) {
        onNext(PgRow.create(dataRow, columns(), dataConverter));
    }

    private void enableAutoRead() {
        context().channel().config().setAutoRead(true);
    }

    private void disableAutoRead() {
        context().channel().config().setAutoRead(false);
    }

    private void readNext() {
        context().channel().read();
    }
}

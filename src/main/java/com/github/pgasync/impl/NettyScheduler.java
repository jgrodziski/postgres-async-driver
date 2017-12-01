package com.github.pgasync.impl;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.BooleanSubscription;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.TimeUnit;

/**
 * Simple bridge for Netty event-loop to RxScheduler.
 * @author Jacek Sokol
 */
@RequiredArgsConstructor(staticName = "forEventExecutor")
public class NettyScheduler extends Scheduler {
    private final EventExecutor eventExecutor;

    @Override
    public Worker createWorker() {
        return new NettyWorker();
    }

    private class NettyWorker extends Worker {
        private final Subscription delegate = new BooleanSubscription();

        @Override
        public Subscription schedule(Action0 action) {
            Future<?> future = eventExecutor.submit(action::call);
            return Subscriptions.from(future);
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            ScheduledFuture<?> future = eventExecutor.schedule(action::call, delayTime, unit);
            return Subscriptions.from(future);
        }

        @Override
        public void unsubscribe() {
            delegate.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return delegate.isUnsubscribed();
        }
    }
}

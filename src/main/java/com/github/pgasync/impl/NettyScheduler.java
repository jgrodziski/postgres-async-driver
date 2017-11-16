package com.github.pgasync.impl;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor(staticName = "forEventExecutor")
public class NettyScheduler extends Scheduler {
    private final EventExecutor eventExecutor;

    @Override
    public Worker createWorker() {
        return new NettyWorker();
    }

    private class NettyWorker extends Worker {
        private volatile boolean unsubscribed;

        @Override
        public Subscription schedule(Action0 action) {
            Future<?> future = eventExecutor.submit(action::call);
            return new NettySubscription(future);
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            ScheduledFuture<?> future = eventExecutor.schedule(action::call, delayTime, unit);
            return new NettySubscription(future);
        }

        @Override
        public void unsubscribe() {
            unsubscribed = true;
        }

        @Override
        public boolean isUnsubscribed() {
            return unsubscribed;
        }
    }

    private class NettySubscription implements Subscription {
        private final Future<?> future;

        NettySubscription(Future<?> future) {
            this.future = future;
        }

        @Override
        public void unsubscribe() {
            future.cancel(true);
        }

        @Override
        public boolean isUnsubscribed() {
            return future.isCancelled();
        }
    }
}

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

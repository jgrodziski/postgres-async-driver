package com.github.pgasync.impl;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;

public abstract class EventExecutorSupport {
    private final EventExecutor eventExecutor;

    protected EventExecutorSupport(EventExecutor eventExecutor) {
        this.eventExecutor = eventExecutor;
    }

    protected Future<?> async(Runnable task) {
        return eventExecutor.submit(task);
    }

    protected EventExecutor eventExecutor() {
        return eventExecutor;
    }
}

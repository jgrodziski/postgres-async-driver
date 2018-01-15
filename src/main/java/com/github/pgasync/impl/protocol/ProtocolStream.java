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

import com.github.pgasync.DatabaseConfig;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.NettyScheduler;
import com.github.pgasync.impl.message.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.*;
import rx.Observable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.nurkiewicz.typeof.TypeOf.whenTypeOf;
import static rx.subscriptions.Subscriptions.create;

/**
 * Protocol stream handler.
 *
 * @author Jacek Sokol
 */
public class ProtocolStream {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolStream.class);

    abstract class PgConsumer implements Consumer<Message> {
        final String query;

        PgConsumer(String query) {
            this.query = query;
        }

        abstract void error(Throwable throwable);

        void closeStream() {
            Completable
                    .fromAction(() -> {
                        LOG.warn("Closing channel due to premature cancellation [{}]", query);
                        subscribers.remove(this);
                        dirty = true;
                        ctx.channel().close();
                    })
                    .subscribeOn(scheduler)
                    .await(1, TimeUnit.SECONDS);
        }
    }

    abstract class ProtocolConsumer<T> extends PgConsumer {
        final SingleSubscriber<T> subscriber;
        final AtomicBoolean done = new AtomicBoolean();

        @SuppressWarnings("unchecked")
        ProtocolConsumer(SingleSubscriber<?> subscriber, String query) {
            super(query);
            this.subscriber = (SingleSubscriber<T>) subscriber;
            subscriber.add(create(ProtocolConsumer.this::unsubscribe));
        }

        void complete(T value) {
            if (!done.get()) {
                done.set(true);
                subscriber.onSuccess(value);
            }
        }

        void complete() {
            complete(null);
        }

        void error(Throwable throwable) {
            done.set(true);
            subscriber.onError(throwable);
        }

        void unsubscribe() {
            if (!done.get()) closeStream();
        }
    }

    abstract class StreamConsumer<T> extends PgConsumer {
        final Emitter<T> subscriber;
        final AtomicBoolean done = new AtomicBoolean();

        @SuppressWarnings("unchecked")
        StreamConsumer(Emitter<T> subscriber, String query) {
            super(query);
            this.subscriber = subscriber;
            subscriber.setSubscription(create(StreamConsumer.this::unsubscribe));
        }

        void complete() {
            if (!done.get()) {
                done.set(true);
                subscriber.onCompleted();
            }
        }

        public void error(Throwable throwable) {
            done.set(true);
            subscriber.onError(throwable);
        }

        void unsubscribe() {
            if (!done.get()) closeStream();
        }
    }

    private final EventLoopGroup group;
    private final DatabaseConfig config;

    private final GenericFutureListener<Future<? super Object>> onError;
    private final Queue<PgConsumer> subscribers = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth
    private final ConcurrentMap<String, List<StreamConsumer<String>>> listeners = new ConcurrentHashMap<>();

    private ChannelHandlerContext ctx;
    private boolean dirty;
    private Scheduler scheduler;

    public ProtocolStream(EventLoopGroup group, DatabaseConfig config) {
        this.group = group;
        this.config = config;
        this.onError = future -> {
            if (!future.isSuccess()) {
                handleError(future.cause());
            }
        };
    }

    public Single<Authentication> connect(StartupMessage startup) {
        return Single.create(subscriber -> {
            ProtocolConsumer<Authentication> consumer = new ProtocolConsumer<Authentication>(subscriber, "CONNECT") {
                @Override
                public void accept(Message message) {
                    whenTypeOf(message)
                            .is(ErrorResponse.class).then(e -> error(toSqlException(e)))
                            .is(ReadyForQuery.class).then(r -> complete(new Authentication(true, null)))
                            .is(Authentication.class).then(this::handleAuthRequest)
                            .orElse(m -> error(new SqlException("Unexpected message at startup stage: " + m)));
                }

                private void handleAuthRequest(Authentication auth) {
                    if (!auth.success()) {
                        subscribers.remove();
                        complete(auth);
                    }
                }
            };

            subscribers.add(consumer);

            InboundChannelInitializer inboundChannelInitializer = new InboundChannelInitializer(startup);
            MessageHandler messageHandler = new MessageHandler(subscribers, listeners, this::handleError);

            new Bootstrap()
                    .group(group)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout())
                    .channel(NioSocketChannel.class)
                    .handler(new ProtocolInitializer(config, inboundChannelInitializer, messageHandler))
                    .connect(config.address())
                    .addListener(onError);
        });
    }

    public Completable authenticate(PasswordMessage password) {
        return Single
                .create(subscriber -> {
                    ProtocolConsumer<Void> consumer = new ProtocolConsumer<Void>(subscriber, "AUTHENTICATE") {
                        @Override
                        public void accept(Message message) {
                            whenTypeOf(message)
                                    .is(ErrorResponse.class).then(e -> error(toSqlException(e)))
                                    .is(Authentication.class).then(this::handleAuthResponse)
                                    .is(ReadyForQuery.class).then(r -> complete());
                        }

                        private void handleAuthResponse(Authentication a) {
                            if (!a.success())
                                error(new SqlException("Failed to authenticate"));
                        }
                    };
                    subscribers.add(consumer);
                    write(password);
                })
                .subscribeOn(scheduler)
                .toCompletable();
    }

    public Observable<Message> command(Message... messages) {
        if (messages.length == 0)
            return Observable.error(new IllegalArgumentException("No messages to send"));
        else if (!isConnected())
            return Observable.error(new IllegalStateException("Channel is closed [" + messages[0] + "]"));

        return Observable.unsafeCreate(BackPressuredEmitter.<Message>create(emitter -> {
            StreamConsumer<Message> consumer = new StreamConsumer<Message>(emitter, messages[0].toString()) {
                SqlException exception;

                @Override
                public void accept(Message message) {
                    whenTypeOf(message)
                            .is(ErrorResponse.class).then(this::handleError)
                            .is(ReadyForQuery.class).then(r -> handleReady())
                            .is(CommandComplete.class).then(this::handleCompletion)
                            .is(Message.class).then(emitter::onNext);
                }

                private void handleCompletion(CommandComplete commandComplete) {
                    enableAutoRead();
                    emitter.onNext(commandComplete);
                }

                private void handleReady() {
                    if (exception == null)
                        complete();
                    else
                        error(exception);
                }

                private void handleError(ErrorResponse e) {
                    exception = toSqlException(e);
                    enableAutoRead();
                }
            };
            subscribers.add(consumer);
            write(messages);
            disableAutoRead();
            readNext();
        }, this::readNext)).subscribeOn(scheduler);
    }

    public Observable<String> listen(String channel) {
        if (!isConnected())
            return Observable.error(new IllegalStateException("Channel is closed [LISTEN]"));

        return Observable.unsafeCreate(BackPressuredEmitter.<String>create(emitter -> {
            StreamConsumer<String> consumer = new StreamConsumer<String>(emitter, "LISTEN") {
                @Override
                public void accept(Message message) {
                    whenTypeOf(message)
                            .is(ErrorResponse.class).then(this::handleError)
                            .is(CommandComplete.class).then(commandComplete -> enableAutoRead())
                            .is(NotificationResponse.class).then(notificationResponse -> emitter.onNext(notificationResponse.payload()));
                }

                private void handleError(ErrorResponse e) {
                    emitter.onError(toSqlException(e));
                    enableAutoRead();
                }

                @Override
                protected void unsubscribe() {
                    enableAutoRead();
                    ctx.executor().submit(() ->
                            Optional.of(listeners.get(channel)).ifPresent(list -> {
                                list.remove(this);
                                if (list.isEmpty())
                                    listeners.remove(channel);
                            })
                    );
                }
            };

            List<StreamConsumer<String>> consumers = listeners.getOrDefault(channel, new LinkedList<>());
            consumers.add(consumer);
            listeners.put(channel, consumers);
            disableAutoRead();
            readNext();
        }, this::readNext)).subscribeOn(scheduler);
    }

    public boolean isConnected() {
        return !dirty && Optional
                .ofNullable(ctx)
                .map(c -> c.channel().isOpen())
                .orElse(false);
    }

    public Completable close() {
        return Completable
                .create(subscriber -> {
                            dirty = true;
                            handleError(new RuntimeException("Closing connection"));
                            ctx.writeAndFlush(Terminate.INSTANCE)
                                    .addListener(closed -> {
                                        if (closed.isSuccess())
                                            subscriber.onCompleted();
                                        else
                                            subscriber.onError(closed.cause());
                                    });
                        }
                )
                .subscribeOn(scheduler);
    }

    private void write(Message... messages) {
        for (Message message : messages) {
            LOG.trace("Writing: {}", message);
            ctx.write(message).addListener(onError);
        }
        ctx.flush();
    }

    private void readNext() {
        ctx.channel().read();
    }

    private void enableAutoRead() {
        ctx.channel().config().setAutoRead(true);
    }

    private void disableAutoRead() {
        ctx.channel().config().setAutoRead(false);
    }

    private void handleError(Throwable throwable) {
        if (!isConnected()) {
            subscribers.forEach(subscriber -> subscriber.error(throwable));
            subscribers.clear();

            listeners.values().stream().flatMap(Collection::stream).forEach(consumer -> consumer.error(throwable));
            listeners.clear();
        } else
            Optional.ofNullable(subscribers.poll()).ifPresent(s -> s.error(throwable));
        dirty = true;
    }

    private SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.level().name(), error.code(), error.message());
    }

    private class InboundChannelInitializer extends ChannelInboundHandlerAdapter {
        private final StartupMessage startup;

        InboundChannelInitializer(StartupMessage startup) {
            this.startup = startup;
        }

        @Override
        public void channelActive(ChannelHandlerContext context) {
            ProtocolStream.this.ctx = context;
            scheduler = NettyScheduler.forEventExecutor(ctx.executor());

            if (config.useSsl())
                write(SSLHandshake.INSTANCE);
            else
                writeStartupAndFixPipeline(context);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object evt) {
            whenTypeOf(evt).is(SslHandshakeCompletionEvent.class).then(e -> {
                if (e.isSuccess())
                    writeStartupAndFixPipeline(context);
                else
                    context.fireExceptionCaught(new SqlException("Failed to initialise SSL"));
            });
        }

        private void writeStartupAndFixPipeline(ChannelHandlerContext context) {
            write(startup);
            context.pipeline().remove(this);
        }
    }

}

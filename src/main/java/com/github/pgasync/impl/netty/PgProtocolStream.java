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

package com.github.pgasync.impl.netty;

import com.github.pgasync.ConnectionConfig;
import com.github.pgasync.SqlException;
import com.github.pgasync.impl.NettyScheduler;
import com.github.pgasync.impl.message.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.nurkiewicz.typeof.TypeOf.whenTypeOf;

/**
 * Netty connection to PostgreSQL backend.
 *
 * @author Antti Laisi
 */
public class PgProtocolStream {
    private static final Logger LOG = LoggerFactory.getLogger(PgProtocolStream.class);

    abstract class AbstractConsumer<T> implements Consumer<Message> {
        final SingleSubscriber<T> subscriber;
        final AtomicBoolean done = new AtomicBoolean();

        @SuppressWarnings("unchecked")
        AbstractConsumer(SingleSubscriber<?> subscriber) {
            this.subscriber = (SingleSubscriber<T>) subscriber;
            subscriber.add(new Subscription() {
                volatile boolean unsubscribed;

                @Override
                public void unsubscribe() {
                    if (!isUnsubscribed()) {
                        unsubscribed = true;
                        AbstractConsumer.this.unsubscribe();
                    }
                }

                @Override
                public boolean isUnsubscribed() {
                    return unsubscribed;
                }
            });
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

        abstract void unsubscribe();
    }

    abstract class ProtocolConsumer<T> extends AbstractConsumer<T> {
        ProtocolConsumer(SingleSubscriber<?> subscriber) {
            super(subscriber);
        }

        @Override
        void unsubscribe() {
            if (!done.get()) closeStream();
        }

        private void closeStream() {
            LOG.warn("Closing channel due to premature cancellation");

            subscribers.remove(this);

            closed = true;
            ctx.channel().close();
        }
    }

    abstract class ChannelConsumer extends AbstractConsumer<Void> {
        final String channel;

        ChannelConsumer(SingleSubscriber<?> subscriber, String channel) {
            super(subscriber);
            this.channel = channel;
        }

        @Override
        void unsubscribe() {
            ctx.executor().submit(() ->
                    Optional.of(listeners.get(channel))
                            .ifPresent(list -> list.remove(this))
            );
        }
    }

    private final EventLoopGroup group;
    private final ConnectionConfig config;

    private final GenericFutureListener<Future<? super Object>> onError;
    private final Queue<ProtocolConsumer<?>> subscribers = new LinkedBlockingDeque<>(); // TODO: limit pipeline queue depth
    private final ConcurrentMap<String, List<ChannelConsumer>> listeners = new ConcurrentHashMap<>();

    private ChannelHandlerContext ctx;
    private boolean closed;
    private Scheduler scheduler;

    public PgProtocolStream(EventLoopGroup group, ConnectionConfig config) {
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
            ProtocolConsumer<Authentication> consumer = new ProtocolConsumer<Authentication>(subscriber) {
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

            new Bootstrap()
                    .group(group)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeout())
                    .channel(NioSocketChannel.class)
                    .handler(new ProtocolInitializer(new InboundChannelInitializer(startup)))
                    .connect(config.address())
                    .addListener(onError);
        });
    }

    public Completable authenticate(PasswordMessage password) {
        return Single
                .create(subscriber -> {
                    ProtocolConsumer<Void> consumer = new ProtocolConsumer<Void>(subscriber) {
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

    public Completable command(StreamHandler handler, Message... messages) {
        if (messages.length == 0)
            return Completable.error(new IllegalArgumentException("No messages to send"));

        return Single
                .create(subscriber -> {
                    ProtocolConsumer<Void> consumer = new ProtocolConsumer<Void>(subscriber) {
                        @Override
                        public void accept(Message message) {
                            whenTypeOf(message)
                                    .is(ErrorResponse.class).then(e -> error(toSqlException(e)))
                                    .is(ReadyForQuery.class).then(r -> complete())
                                    .is(CommandComplete.class).then(handler::complete)
                                    .is(RowDescription.class).then(handler::rowDescription)
                                    .is(DataRow.class).then(handler::dataRow);
                        }
                    };

                    if (!isConnected())
                        consumer.error(new IllegalStateException("Channel is closed [" + messages[0] + "]"));
                    else
                        ensureInLoop(() -> {
                            subscribers.add(consumer);
                            handler.init(ctx);
                            write(messages);
                        });
                })
                .toCompletable();
    }

    public Completable listen(Consumer<String> consumer, String channel) {
        return Single
                .create(subscriber -> {
                    ChannelConsumer channelConsumer = new ChannelConsumer(subscriber, channel) {
                        @Override
                        public void accept(Message message) {
                            whenTypeOf(message)
                                    .is(ErrorResponse.class).then(e -> error(toSqlException(e)))
                                    .is(NotificationResponse.class).then(n -> consumer.accept(n.payload()));
                        }
                    };

                    List<ChannelConsumer> consumers = listeners.getOrDefault(channel, new LinkedList<>());
                    consumers.add(channelConsumer);
                    listeners.put(channel, consumers);
                })
                .subscribeOn(scheduler)
                .toCompletable();
    }

    public boolean isConnected() {
        return !closed && Optional
                .ofNullable(ctx)
                .map(c -> c.channel().isOpen())
                .orElse(false);
    }

    public Completable close() {
        return Completable
                .create(subscriber ->
                        ctx.writeAndFlush(Terminate.INSTANCE)
                                .addListener(closed -> {
                                    if (closed.isSuccess())
                                        subscriber.onCompleted();
                                    else
                                        subscriber.onError(closed.cause());
                                })
                )
                .subscribeOn(scheduler);
    }

    private void ensureInLoop(Runnable runnable) {
        if (ctx.executor().inEventLoop())
            runnable.run();
        else
            ctx.executor().submit(runnable);
    }

    private void write(Message... messages) {
        for (Message message : messages) {
            LOG.trace("Writing: {}", message);
            ctx.write(message).addListener(onError);
        }
        ctx.flush();
    }

    private void publishNotification(NotificationResponse notification) {
        Optional.of(listeners.get(notification.channel()))
                .ifPresent(consumers ->
                        consumers.forEach(c -> c.accept(notification))
                );
    }

    private void handleError(Throwable throwable) {
        if (!isConnected()) {
            subscribers.forEach(subscriber -> subscriber.error(throwable));
            subscribers.clear();

            listeners.values().stream().flatMap(Collection::stream).forEach(consumer -> consumer.error(throwable));
            listeners.clear();
        } else
            Optional.ofNullable(subscribers.poll()).ifPresent(s -> s.error(throwable));
    }

    private SqlException toSqlException(ErrorResponse error) {
        return new SqlException(error.level().name(), error.code(), error.message());
    }

    @AllArgsConstructor
    private class ProtocolInitializer extends ChannelInitializer<Channel> {
        private final ChannelHandler onActive;

        @Override
        protected void initChannel(Channel channel) throws Exception {
            if (config.useSsl())
                channel.pipeline().addLast(new SslInitiator());

            channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
            channel.pipeline().addLast(new ByteBufMessageDecoder());
            channel.pipeline().addLast(new ByteBufMessageEncoder());
            channel.pipeline().addLast(new FlowControlHandler(true));
            channel.pipeline().addLast(new ProtocolHandler());
            channel.pipeline().addLast(onActive);
        }
    }

    private class InboundChannelInitializer extends ChannelInboundHandlerAdapter {
        private final StartupMessage startup;

        InboundChannelInitializer(StartupMessage startup) {
            this.startup = startup;
        }

        @Override
        public void channelActive(ChannelHandlerContext context) {
            PgProtocolStream.this.ctx = context;
            scheduler = NettyScheduler.forEventExecutor(ctx.executor());

            if (config.useSsl())
                write(SSLHandshake.INSTANCE);
            else
                writeStartupAndFixPipeline(context);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object evt) throws Exception {
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

    private class ProtocolHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {
            LOG.trace("Reading: {}", msg);

            whenTypeOf(msg)
                    .is(NotificationResponse.class).then(PgProtocolStream.this::publishNotification)
                    .is(ReadyForQuery.class).then(r -> subscribers.poll().accept(r))
                    .is(Message.class).then(m -> subscribers.peek().accept(m));
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            if (!subscribers.isEmpty())
                exceptionCaught(context, new IOException("Channel state changed to inactive"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            handleError(cause);
        }
    }
}

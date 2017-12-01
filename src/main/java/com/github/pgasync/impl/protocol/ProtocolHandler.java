package com.github.pgasync.impl.protocol;

import com.github.pgasync.impl.message.Message;
import com.github.pgasync.impl.message.NotificationResponse;
import com.github.pgasync.impl.message.ReadyForQuery;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

import static com.nurkiewicz.typeof.TypeOf.whenTypeOf;

/**
 * Netty handler that handles incoming messages.
 * @author Jacek Sokol
 */
@AllArgsConstructor
class ProtocolHandler extends ChannelInboundHandlerAdapter {
    private final Logger LOG = LoggerFactory.getLogger(ProtocolHandler.class);

    private final Queue<ProtocolStream.PgConsumer> subscribers;
    private final Map<String, List<ProtocolStream.StreamConsumer<String>>> listeners;
    private final Consumer<Throwable> errorHandler;

    @Override
    public void channelRead(ChannelHandlerContext context, Object msg) throws Exception {
        LOG.trace("Reading: {}", msg);

        whenTypeOf(msg)
                .is(NotificationResponse.class).then(this::publishNotification)
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
        errorHandler.accept(cause);
    }

    private void publishNotification(NotificationResponse notification) {
        Optional.of(listeners.get(notification.channel()))
                .ifPresent(consumers -> consumers.forEach(c -> c.accept(notification)));
    }
}

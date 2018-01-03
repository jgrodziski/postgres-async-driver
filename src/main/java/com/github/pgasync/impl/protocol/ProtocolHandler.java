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
    public void channelRead(ChannelHandlerContext context, Object msg) {
        LOG.trace("Reading: {}", msg);

        whenTypeOf(msg)
                .is(NotificationResponse.class).then(this::publishNotification)
                .is(ReadyForQuery.class).then(r -> subscribers.poll().accept(r))
                .is(Message.class).then(m -> subscribers.peek().accept(m));
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) {
        if (!subscribers.isEmpty())
            exceptionCaught(context, new IOException("Channel state changed to inactive"));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        errorHandler.accept(cause);
    }

    private void publishNotification(NotificationResponse notification) {
        Optional.of(listeners.get(notification.channel()))
                .ifPresent(consumers -> consumers.forEach(c -> c.accept(notification)));
    }
}

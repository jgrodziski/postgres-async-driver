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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flow.FlowControlHandler;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class ProtocolInitializer extends ChannelInitializer<Channel> {
    private final DatabaseConfig config;
    private final ChannelHandler onActive;
    private final ChannelHandler protocolHandler;

    @Override
    protected void initChannel(Channel channel) throws Exception {
        if (config.useSsl())
            channel.pipeline().addLast(new SslInitiator());

        channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 1, 4, -4, 0, true));
        channel.pipeline().addLast(new ByteBufMessageDecoder());
        channel.pipeline().addLast(new ByteBufMessageEncoder());
        channel.pipeline().addLast(new FlowControlHandler(true));
        channel.pipeline().addLast(protocolHandler);
        channel.pipeline().addLast(onActive);
    }
}

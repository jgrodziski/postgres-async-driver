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

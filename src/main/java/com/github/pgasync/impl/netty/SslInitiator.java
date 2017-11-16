package com.github.pgasync.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.List;

class SslInitiator extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 1) {
            return;
        }
        if ('S' != in.readByte()) {
            ctx.fireExceptionCaught(new IllegalStateException("SSL required but not supported by backend server"));
            return;
        }
        ctx.pipeline().remove(this);
        ctx.pipeline().addFirst(
                SslContextBuilder
                        .forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build()
                        .newHandler(ctx.alloc())
        );
    }
}

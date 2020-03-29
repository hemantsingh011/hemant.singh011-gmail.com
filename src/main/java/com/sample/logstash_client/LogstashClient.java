package com.sample.logstash_client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.CompleteFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LogstashClient {

    private AtomicReference<Channel> channelAtomicReference = new AtomicReference<>();
    private AtomicBoolean isConnected = new AtomicBoolean(false);

    CompletableFuture<Boolean> connect() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap(); // (1)
        b.group(workerGroup); // (2)
        b.channel(NioSocketChannel.class); // (3)
        b.remoteAddress(new InetSocketAddress("localhost", 6060));
        b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
        ChannelInitializer channelInitializer = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new ClientHandler());
                channelAtomicReference.set(ch);
                isConnected.set(true);
                System.out.println("Channel initialized");
            }
        };
        b.handler(channelInitializer);

        // Start the client.
        ChannelFuture connect = b.connect();
        connect.addListener(future -> {
            if (!future.isSuccess()) {
                isConnected.set(false);
                result.completeExceptionally(future.cause());
            } else {
                System.out.println("Connected");
                result.complete(true);
            }
        });
        return result;
    }

    public void sendMessage(String message) {
        if (isConnected.get() && channelAtomicReference.get() != null && channelAtomicReference.get().isActive()) {
            System.out.println("Sending message");
            final ByteBuf outgoingMessage = channelAtomicReference.get().alloc().directBuffer();
            outgoingMessage.writeBytes(message.getBytes(StandardCharsets.UTF_8));
            channelAtomicReference.get().writeAndFlush(outgoingMessage);
            System.out.println("message sent");

        }
    }
}

package com.mcorp.MChat.server;

import com.mcorp.MChat.protocol.MessageDecoder;
import com.mcorp.MChat.protocol.MessageEncoder;
import com.mcorp.MChat.server.handler.HeartbeatHandler;
import com.mcorp.MChat.server.handler.ServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.util.concurrent.TimeUnit;
@Slf4j
@Component
public class NettyServer {

    @Value("${mchat.netty.port:8888}")
    private int port;

    @Value("${mchat.netty.reader-idle-time:60}")
    private int readerIdleTime;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new IdleStateHandler(readerIdleTime, 0, 0, TimeUnit.SECONDS))
                                    .addLast(new MessageDecoder())
                                    .addLast(new MessageEncoder())
                                    .addLast(new HeartbeatHandler())
                                    .addLast(new ServerHandler());
                        }
                    });

            ChannelFuture f = b.bind(port).sync();
            serverChannel = f.channel();
            log.info("MChat 服务器已在端口 {} 准备就绪，等待连接...", port);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("JVM shutdown hook triggered, closing Netty server...");
                shutdown();
            }));

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.info("Netty server thread interrupted, shutting down...");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("服务器启动发生致命故障: ", e);
        } finally {
            shutdown();
        }
    }

    @PreDestroy
    public void onDestroy() {
        log.info("Spring @PreDestroy triggered, shutting down Netty server...");
        shutdown();
    }

    private void shutdown() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
        log.info("Netty server resources released.");
    }
}

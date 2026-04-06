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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.util.concurrent.TimeUnit;
@Slf4j
@Component
public class NettyServer {

    @Value("${mchat.netty.port:8888}")
    private int port; // 建议从 application.yml 读取

    @Value("${mchat.netty.reader-idle-time:60}")
    private int readerIdleTime;

    public void start() {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_KEEPALIVE, true) // 开启 TCP 底层心跳
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    // --- [ 1. 计时器 ] ---
                                    // 60秒没收到客户端数据，会触发 READER_IDLE 事件
                                    .addLast(new IdleStateHandler(readerIdleTime, 0, 0, TimeUnit.SECONDS))

                                    // --- [ 2. 协议编解码 ] ---
                                    .addLast(new MessageDecoder()) // 二进制 -> Message 对象
                                    .addLast(new MessageEncoder()) // Message 对象 -> 二进制

                                    // --- [ 3. 稳定性保障 ] ---
                                    // 接收到计时器的信号后，执行清理逻辑
                                    .addLast(new HeartbeatHandler())

                                    // --- [ 4. 业务处理器 ] ---
                                    // 处理登录、聊天消息 (我们马上要写这个)
                                    .addLast(new ServerHandler());
                        }
                    });

            ChannelFuture f = b.bind(port).sync();
            log.info("MChat 服务器已在端口 {} 准备就绪，等待连接...", port);
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("服务器启动发生致命故障: ", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
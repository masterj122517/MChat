package com.mcorp.MChat.server.handler;

import com.mcorp.MChat.server.SessionManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // 1. 判断是否为 Netty 的 空闲事件
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleEvent = (IdleStateEvent) evt;
            // 2. 只处理读空闲事件（即客户端长时间未发送数据）
            if (idleEvent.state() == IdleState.READER_IDLE) {
                String username = ctx.channel().attr(SessionManager.ATTR_USER).get();
                log.warn("检测到用户 {} 长时间未发送数据，连接将被关闭。", username);
                ctx.close(); // 3. 关闭连接，触发 SessionManager 的移除逻辑
            }
        } else {
            // 4. 其他事件继续传递
            super.userEventTriggered(ctx, evt);
        }

    }


}

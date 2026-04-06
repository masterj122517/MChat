package com.mcorp.MChat.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        try {
            // 1. 基础验证：确保消息体不为 null
            if (msg.getBody() == null) {
                log.error("消息体 (Body) 为空，拒绝编码。指令类型: {}", msg.getCommand());
                throw new IllegalArgumentException("Message body cannot be null");
            }

            // 2. 写入魔数 (4字节)
            out.writeInt(msg.getMagicNumber());
            // 3. 写入版本 (1字节)
            out.writeByte(msg.getVersion());
            // 4. 写入序列化方式 (1字节)
            out.writeByte(msg.getSerializer());
            // 5. 写入指令 (1字节)
            out.writeByte(msg.getCommand());

            // 6. 写入长度 (4字节)
            byte[] data = msg.getBody();
            out.writeInt(data.length);

            // 7. 写入实际数据
            out.writeBytes(data);

        } catch (Exception e) {
            log.error("编码过程中发生意外错误: ", e);
            // 将异常继续抛出，Netty 的 Pipeline 会捕获并交由 exceptionCaught 处理
            throw e;
        }
    }
}
package com.mcorp.MChat.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;


/**
 * 由于 MessageDecoder 需要处理粘包和半包问题，我们选择继承 LengthFieldBasedFrameDecoder 来简化这一过程
 * This class will handle decoding byte arrays back into Message objects after reception
 */
@Slf4j
public class MessageDecoder extends LengthFieldBasedFrameDecoder {

/**
 * 配置解码器参数，解决粘包/拆包
 * * maxFrameLength: 1MB (防止恶意大包撑爆内存)
 * lengthFieldOffset: 魔数(4) + 版本(1) + 序列化(1) + 指令(1) = 7
 * lengthFieldLength: 长度字段(length)占 4 字节
 */
public MessageDecoder() {
    super(1024 * 1024, 7, 4);
}

@Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    // call super.decode to handle frame decoding based on length field
    ByteBuf frame = (ByteBuf) super.decode(ctx, in);
    if (frame == null) {
        return null; // 数据没有攒够一个完整的包，继续等待
    }
    try {
        int magic = frame.readInt();
        if (magic != ProtocolConstants.MAGIC_NUMBER) {
            log.error("无效的魔数: {}, 连接将被关闭", magic);
            ctx.close();
            return null;
        }
        // 继续读取版本、序列化方式和指令
        Message message = new Message();
        message.setMagicNumber(magic);
        message.setVersion(frame.readByte());
        message.setSerializer(frame.readByte());
        message.setCommand(frame.readByte());

        // read length and body
        int length = frame.readInt();
        if (length < 0 || length > 1024 * 1024) {
            log.error("非法的消息长度: {}, 连接将被关闭", length);
            ctx.close();
            return null;
        }
        message.setLength(length);

        byte[] body = new byte[length];
        frame.readBytes(body);
        message.setBody(body);
        log.info("成功解码消息: 指令={}, 长度={}", message.getCommand(), length);
        return message;
    } catch (Exception e) {
        log.error("解码过程中发生错误: ", e);
        // 发生异常时关闭连接，防止恶意攻击
        ctx.close();
        return null;
    } finally {
        frame.release(); // 释放 ByteBuf 资源
    }

}

}

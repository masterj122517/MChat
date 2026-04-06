package com.mcorp.MChat.client;

import com.mcorp.MChat.protocol.*;
import com.mcorp.MChat.protocol.ChatModel.MessageRequest;
import com.mcorp.MChat.protocol.ChatModel.MessageType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test client for MChat server.
 * Demonstrates: Login, P2P chat, Group operations, Heartbeat, and ACK handling.
 *
 * Usage: Run two instances with different user IDs to test P2P chat.
 *   - Instance 1: USER_ID=1001, USERNAME="John"
 *   - Instance 2: USER_ID=1002, USERNAME="Jane"
 */
public class ChatClient {

    private static final long USER_ID = 1001;
    private static final String USERNAME = "John";
    private static final String SERVER_HOST = "127.0.0.1";
    private static final int SERVER_PORT = 8888;
    private static final AtomicLong MSG_ID = new AtomicLong(USER_ID * 100000);

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new MessageEncoder());
                        ch.pipeline().addLast(new MessageDecoder());
                        ch.pipeline().addLast(new ClientHandler());
                    }
                });

            ChannelFuture f = b.connect(SERVER_HOST, SERVER_PORT).sync();
            Channel channel = f.channel();

            // === 1. LOGIN ===
            sendLogin(channel);
            Thread.sleep(1000);

            // === 2. P2P CHAT ===
            sendP2PChat(channel, 1002, "Hello from MChat! This is a P2P message.");
            Thread.sleep(500);

            // === 3. CREATE GROUP ===
            sendGroupOp(channel, "group-001", MessageType.CREATE_GROUP);
            Thread.sleep(500);

            // === 4. SEND GROUP MESSAGE ===
            sendGroupChat(channel, "group-001", "Hello group! This is a broadcast.");
            Thread.sleep(500);

            // === 5. HEARTBEAT ===
            sendHeartbeat(channel);
            Thread.sleep(500);

            // === 6. SEND ANOTHER P2P (to offline user, will be stored in Redis) ===
            sendP2PChat(channel, 9999, "This message should be stored offline.");
            Thread.sleep(500);

            System.out.println("[Client] All test messages sent. Waiting for responses...");
            System.out.println("[Client] Press Ctrl+C to disconnect.");

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    // --- Message builders ---

    private static void sendLogin(Channel channel) {
        MessageRequest loginReq = MessageRequest.newBuilder()
                .setFromId(USER_ID)
                .setType(MessageType.LOGIN)
                .setContent(USERNAME)
                .setTimestamp(System.currentTimeMillis())
                .setMessageId(MSG_ID.incrementAndGet())
                .build();

        Message msg = new Message();
        msg.setCommand(ProtocolConstants.COMMAND_LOGIN);
        msg.setBody(loginReq.toByteArray());
        channel.writeAndFlush(msg);
        System.out.println("[Client] Login sent as " + USERNAME + " (ID: " + USER_ID + ")");
    }

    private static void sendP2PChat(Channel channel, long toId, String content) {
        MessageRequest chatReq = MessageRequest.newBuilder()
                .setFromId(USER_ID)
                .setToId(toId)
                .setType(MessageType.CHAT_P2P)
                .setContent(content)
                .setTimestamp(System.currentTimeMillis())
                .setMessageId(MSG_ID.incrementAndGet())
                .build();

        Message msg = new Message();
        msg.setCommand(ProtocolConstants.COMMAND_CHAT);
        msg.setBody(chatReq.toByteArray());
        channel.writeAndFlush(msg);
        System.out.println("[Client] P2P message sent to user " + toId + ": " + content);
    }

    private static void sendGroupChat(Channel channel, String groupId, String content) {
        MessageRequest chatReq = MessageRequest.newBuilder()
                .setFromId(USER_ID)
                .setType(MessageType.CHAT_GROUP)
                .setGroupId(groupId)
                .setContent(content)
                .setTimestamp(System.currentTimeMillis())
                .setMessageId(MSG_ID.incrementAndGet())
                .build();

        Message msg = new Message();
        msg.setCommand(ProtocolConstants.COMMAND_CHAT);
        msg.setBody(chatReq.toByteArray());
        channel.writeAndFlush(msg);
        System.out.println("[Client] Group message sent to " + groupId + ": " + content);
    }

    private static void sendGroupOp(Channel channel, String groupId, MessageType opType) {
        MessageRequest req = MessageRequest.newBuilder()
                .setFromId(USER_ID)
                .setType(opType)
                .setGroupId(groupId)
                .setTimestamp(System.currentTimeMillis())
                .setMessageId(MSG_ID.incrementAndGet())
                .build();

        Message msg = new Message();
        msg.setCommand(ProtocolConstants.COMMAND_GROUP_OP);
        msg.setBody(req.toByteArray());
        channel.writeAndFlush(msg);
        System.out.println("[Client] Group operation " + opType + " sent for group " + groupId);
    }

    private static void sendHeartbeat(Channel channel) {
        MessageRequest hb = MessageRequest.newBuilder()
                .setFromId(USER_ID)
                .setType(MessageType.LOGIN) // type doesn't matter for heartbeat
                .setTimestamp(System.currentTimeMillis())
                .build();

        Message msg = new Message();
        msg.setCommand(ProtocolConstants.COMMAND_HEARTBEAT);
        msg.setBody(hb.toByteArray());
        channel.writeAndFlush(msg);
        System.out.println("[Client] Heartbeat sent.");
    }

    // --- Client-side handler to process server responses ---

    private static class ClientHandler extends SimpleChannelInboundHandler<Message> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
            byte command = msg.getCommand();
            try {
                ChatModel.MessageRequest request = ChatModel.MessageRequest.parseFrom(msg.getBody());

                switch (command) {
                    case ProtocolConstants.COMMAND_LOGIN_RESP:
                        System.out.println("[Client] LOGIN RESPONSE: " + request.getContent());
                        break;
                    case ProtocolConstants.COMMAND_ACK:
                        System.out.println("[Client] ACK received for msgId=" + request.getMessageId()
                                + " -> " + request.getContent());
                        // In production, client would send ACK back to confirm receipt
                        break;
                    case ProtocolConstants.COMMAND_CHAT:
                        System.out.println("[Client] CHAT from user " + request.getFromId()
                                + ": " + request.getContent());
                        // Send ACK back to server to confirm we received the message
                        sendClientAck(ctx.channel(), request.getMessageId());
                        break;
                    case ProtocolConstants.COMMAND_GROUP_OP:
                        System.out.println("[Client] GROUP RESPONSE for group " + request.getGroupId()
                                + ": " + request.getContent());
                        break;
                    default:
                        System.out.println("[Client] Unknown command " + command
                                + ", content: " + request.getContent());
                }
            } catch (Exception e) {
                System.err.println("[Client] Error parsing server message: " + e.getMessage());
            }
        }

        private void sendClientAck(Channel channel, long messageId) {
            try {
                MessageRequest ack = MessageRequest.newBuilder()
                        .setFromId(USER_ID)
                        .setMessageId(messageId)
                        .setType(MessageType.ACK)
                        .setContent("received")
                        .setTimestamp(System.currentTimeMillis())
                        .build();

                Message msg = new Message();
                msg.setCommand(ProtocolConstants.COMMAND_ACK);
                msg.setBody(ack.toByteArray());
                channel.writeAndFlush(msg);
                System.out.println("[Client] ACK sent for messageId=" + messageId);
            } catch (Exception e) {
                System.err.println("[Client] Failed to send ACK: " + e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("[Client] Connection error: " + cause.getMessage());
            ctx.close();
        }
    }
}

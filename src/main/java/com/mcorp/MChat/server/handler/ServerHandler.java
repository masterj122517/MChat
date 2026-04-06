package com.mcorp.MChat.server.handler;

import com.mcorp.MChat.protocol.ChatModel;
import com.mcorp.MChat.protocol.Message;
import com.mcorp.MChat.protocol.ProtocolConstants;
import com.mcorp.MChat.server.BusinessThreadPool;
import com.mcorp.MChat.server.MetricsMonitor;
import com.mcorp.MChat.server.RedisRouter;
import com.mcorp.MChat.server.SessionManager;
import com.mcorp.MChat.utils.RedisUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class ServerHandler extends SimpleChannelInboundHandler<Message> {

    // Group management: groupId -> ChannelGroup
    private static final ConcurrentHashMap<String, ChannelGroup> GROUP_CHANNELS = new ConcurrentHashMap<>();

    // Group membership: groupId -> Set<username> (for tracking members across reconnections)
    private static final ConcurrentHashMap<String, Set<String>> GROUP_MEMBERS = new ConcurrentHashMap<>();

    // Simple message ID generator for server-originated messages
    private static final AtomicLong MESSAGE_ID_GEN = new AtomicLong(System.currentTimeMillis());

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        MetricsMonitor.incrementConnections();
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        MetricsMonitor.incrementMessagesIn();
        byte command = msg.getCommand();

        switch (command) {
            case ProtocolConstants.COMMAND_LOGIN:
                handleLogin(ctx, msg);
                break;
            case ProtocolConstants.COMMAND_CHAT:
                handleChat(ctx, msg);
                break;
            case ProtocolConstants.COMMAND_HEARTBEAT:
                handleHeartbeat(ctx, msg);
                break;
            case ProtocolConstants.COMMAND_ACK:
                handleAck(ctx, msg);
                break;
            case ProtocolConstants.COMMAND_GROUP_OP:
                handleGroupOperation(ctx, msg);
                break;
            default:
                log.warn("Received unknown command: {}", command);
        }
    }

    // ========== LOGIN ==========

    private void handleLogin(ChannelHandlerContext ctx, Message msg) {
        try {
            ChatModel.MessageRequest loginReq = ChatModel.MessageRequest.parseFrom(msg.getBody());
            String username = loginReq.getContent();
            SessionManager.addSession(username, ctx.channel());
            log.info("User {} logged in successfully.", username);

            // Register user in Redis for cross-server routing
            RedisRouter.registerUserOnline(username);

            // Send login response back to client
            sendLoginResponse(ctx.channel(), username, true, "Login successful");

            // Re-join any groups the user was a member of
            rejoinGroups(username, ctx.channel());

            // Deliver offline messages stored in Redis
            deliverOfflineMessages(ctx.channel(), username);

        } catch (Exception e) {
            log.error("Error processing login message: ", e);
            sendLoginResponse(ctx.channel(), "", false, "Login failed: " + e.getMessage());
        }
    }

    private void sendLoginResponse(Channel channel, String username, boolean success, String message) {
        try {
            ChatModel.MessageRequest resp = ChatModel.MessageRequest.newBuilder()
                    .setFromId(0) // Server
                    .setType(ChatModel.MessageType.LOGIN_RESP)
                    .setContent(success ? "OK:" + username + ":" + message : "FAIL:" + message)
                    .setTimestamp(System.currentTimeMillis())
                    .setMessageId(MESSAGE_ID_GEN.incrementAndGet())
                    .build();

            Message respMsg = new Message();
            respMsg.setCommand(ProtocolConstants.COMMAND_LOGIN_RESP);
            respMsg.setBody(resp.toByteArray());
            channel.writeAndFlush(respMsg);
        } catch (Exception e) {
            log.error("Failed to send login response: ", e);
        }
    }

    // ========== P2P CHAT ==========

    private void handleChat(ChannelHandlerContext ctx, Message msg) {
        try {
            ChatModel.MessageRequest request = ChatModel.MessageRequest.parseFrom(msg.getBody());
            ChatModel.MessageType type = request.getType();

            if (type == ChatModel.MessageType.CHAT_GROUP) {
                handleGroupChat(ctx, msg, request);
            } else {
                handleP2PChat(ctx, msg, request);
            }
        } catch (Exception e) {
            log.error("Error processing chat message: ", e);
        }
    }

    private void handleP2PChat(ChannelHandlerContext ctx, Message msg, ChatModel.MessageRequest request) {
        long toId = request.getToId();
        long messageId = request.getMessageId();

        BusinessThreadPool.submit(() -> {
            Channel targetChannel = SessionManager.getSession(String.valueOf(toId));

            if (targetChannel != null && targetChannel.isActive()) {
                // Target user is on this server, deliver directly
                targetChannel.writeAndFlush(msg);
                MetricsMonitor.incrementMessagesOut();
                log.info("Message {} forwarded to user {} in real-time", messageId, toId);
                sendAck(ctx.channel(), messageId, request.getFromId(), toId, true, "Delivered");

            } else if (RedisRouter.routeToRemote(String.valueOf(toId), msg)) {
                // Target user is on another server, routed via Redis Pub/Sub
                MetricsMonitor.incrementMessagesOut();
                log.info("Message {} routed to remote server for user {}", messageId, toId);
                sendAck(ctx.channel(), messageId, request.getFromId(), toId, true, "Routed to remote");

            } else {
                // Target user is offline everywhere, store in Redis offline queue
                log.warn("User {} is offline, storing message {} in Redis offline queue...", toId, messageId);
                storeOfflineMessage(String.valueOf(toId), msg);
                sendAck(ctx.channel(), messageId, request.getFromId(), toId, true, "Stored offline");
            }
        });
    }

    // ========== GROUP CHAT ==========

    private void handleGroupChat(ChannelHandlerContext ctx, Message msg, ChatModel.MessageRequest request) {
        String groupId = request.getGroupId();
        long messageId = request.getMessageId();

        if (groupId == null || groupId.isEmpty()) {
            log.warn("Group chat message missing groupId from user {}", request.getFromId());
            sendAck(ctx.channel(), messageId, request.getFromId(), 0, false, "Missing groupId");
            return;
        }

        BusinessThreadPool.submit(() -> {
            ChannelGroup group = GROUP_CHANNELS.get(groupId);
            if (group != null) {
                // Broadcast to all group members (Netty ChannelGroup automatically skips closed channels)
                group.writeAndFlush(msg);
                log.info("Message {} broadcast to group {}", messageId, groupId);

                // Store offline messages for group members who are not online
                storeGroupOfflineMessages(groupId, msg, String.valueOf(request.getFromId()));

                sendAck(ctx.channel(), messageId, request.getFromId(), 0, true, "Group broadcast sent");
            } else {
                log.warn("Group {} does not exist", groupId);
                sendAck(ctx.channel(), messageId, request.getFromId(), 0, false, "Group not found");
            }
        });
    }

    private void storeGroupOfflineMessages(String groupId, Message msg, String senderUsername) {
        Set<String> members = GROUP_MEMBERS.get(groupId);
        if (members == null) return;

        for (String member : members) {
            // Skip the sender
            if (member.equals(senderUsername)) continue;

            Channel ch = SessionManager.getSession(member);
            if (ch == null || !ch.isActive()) {
                storeOfflineMessage(member, msg);
            }
        }
    }

    // ========== GROUP OPERATIONS ==========

    private void handleGroupOperation(ChannelHandlerContext ctx, Message msg) {
        try {
            ChatModel.MessageRequest request = ChatModel.MessageRequest.parseFrom(msg.getBody());
            ChatModel.MessageType type = request.getType();
            String groupId = request.getGroupId();
            String username = ctx.channel().attr(SessionManager.ATTR_USER).get();

            if (username == null) {
                log.warn("Unregistered user attempted group operation");
                return;
            }

            switch (type) {
                case CREATE_GROUP:
                    createGroup(groupId, username, ctx.channel());
                    break;
                case JOIN_GROUP:
                    joinGroup(groupId, username, ctx.channel());
                    break;
                case LEAVE_GROUP:
                    leaveGroup(groupId, username, ctx.channel());
                    break;
                default:
                    log.warn("Unknown group operation type: {}", type);
            }
        } catch (Exception e) {
            log.error("Error processing group operation: ", e);
        }
    }

    private void createGroup(String groupId, String username, Channel channel) {
        if (GROUP_CHANNELS.containsKey(groupId)) {
            log.warn("Group {} already exists, creation rejected", groupId);
            sendGroupResponse(channel, groupId, "CREATE_FAIL: Group already exists");
            return;
        }

        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        group.add(channel);
        GROUP_CHANNELS.put(groupId, group);

        Set<String> members = ConcurrentHashMap.newKeySet();
        members.add(username);
        GROUP_MEMBERS.put(groupId, members);

        log.info("User {} created group {}", username, groupId);
        sendGroupResponse(channel, groupId, "CREATE_OK");
    }

    private void joinGroup(String groupId, String username, Channel channel) {
        ChannelGroup group = GROUP_CHANNELS.get(groupId);
        if (group == null) {
            log.warn("Group {} does not exist, cannot join", groupId);
            sendGroupResponse(channel, groupId, "JOIN_FAIL: Group not found");
            return;
        }

        group.add(channel);
        GROUP_MEMBERS.computeIfAbsent(groupId, k -> ConcurrentHashMap.newKeySet()).add(username);

        log.info("User {} joined group {}", username, groupId);
        sendGroupResponse(channel, groupId, "JOIN_OK");
    }

    private void leaveGroup(String groupId, String username, Channel channel) {
        ChannelGroup group = GROUP_CHANNELS.get(groupId);
        if (group == null) {
            log.warn("Group {} does not exist, cannot leave", groupId);
            sendGroupResponse(channel, groupId, "LEAVE_FAIL: Group not found");
            return;
        }

        group.remove(channel);
        Set<String> members = GROUP_MEMBERS.get(groupId);
        if (members != null) {
            members.remove(username);
            if (members.isEmpty()) {
                GROUP_CHANNELS.remove(groupId);
                GROUP_MEMBERS.remove(groupId);
                log.info("Group {} dissolved (no members left)", groupId);
            }
        }

        log.info("User {} left group {}", username, groupId);
        sendGroupResponse(channel, groupId, "LEAVE_OK");
    }

    private void rejoinGroups(String username, Channel channel) {
        GROUP_MEMBERS.forEach((groupId, members) -> {
            if (members.contains(username)) {
                ChannelGroup group = GROUP_CHANNELS.get(groupId);
                if (group != null) {
                    group.add(channel);
                    log.info("User {} re-joined group {} after reconnection", username, groupId);
                }
            }
        });
    }

    private void sendGroupResponse(Channel channel, String groupId, String result) {
        try {
            ChatModel.MessageRequest resp = ChatModel.MessageRequest.newBuilder()
                    .setFromId(0)
                    .setType(ChatModel.MessageType.ACK)
                    .setGroupId(groupId)
                    .setContent(result)
                    .setTimestamp(System.currentTimeMillis())
                    .setMessageId(MESSAGE_ID_GEN.incrementAndGet())
                    .build();

            Message respMsg = new Message();
            respMsg.setCommand(ProtocolConstants.COMMAND_GROUP_OP);
            respMsg.setBody(resp.toByteArray());
            channel.writeAndFlush(respMsg);
        } catch (Exception e) {
            log.error("Failed to send group response: ", e);
        }
    }

    // ========== ACK MECHANISM ==========

    private void handleAck(ChannelHandlerContext ctx, Message msg) {
        try {
            ChatModel.MessageRequest ack = ChatModel.MessageRequest.parseFrom(msg.getBody());
            log.info("Received ACK from user {} for messageId {}", ack.getFromId(), ack.getMessageId());
            // In a production system, this would mark the message as delivered in a persistent store
            // and potentially stop retry timers. For now, we just log it.
        } catch (Exception e) {
            log.error("Error processing ACK: ", e);
        }
    }

    private void sendAck(Channel channel, long messageId, long fromId, long toId, boolean success, String detail) {
        try {
            ChatModel.MessageRequest ack = ChatModel.MessageRequest.newBuilder()
                    .setMessageId(messageId)
                    .setFromId(0) // Server
                    .setToId(fromId)
                    .setType(ChatModel.MessageType.ACK)
                    .setContent(success ? "ACK_OK:" + detail : "ACK_FAIL:" + detail)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            Message ackMsg = new Message();
            ackMsg.setCommand(ProtocolConstants.COMMAND_ACK);
            ackMsg.setBody(ack.toByteArray());
            channel.writeAndFlush(ackMsg);
        } catch (Exception e) {
            log.error("Failed to send ACK for messageId {}: ", messageId, e);
        }
    }

    // ========== HEARTBEAT ==========

    private void handleHeartbeat(ChannelHandlerContext ctx, Message msg) {
        // IdleStateHandler resets automatically when data is received; nothing else needed
        log.debug("Received heartbeat from {}", ctx.channel().remoteAddress());
    }

    // ========== OFFLINE MESSAGES (Redis) ==========

    private void storeOfflineMessage(String userId, Message msg) {
        try {
            RedisTemplate<String, Object> redis = RedisUtil.getRedis();
            if (redis != null) {
                String redisKey = "OFFLINE_MSG_" + userId;
                redis.opsForList().rightPush(redisKey, msg.getBody());
                log.debug("Stored offline message for user {} in Redis", userId);
            } else {
                log.warn("Redis is unavailable, offline message for user {} is lost!", userId);
            }
        } catch (Exception e) {
            log.error("Failed to store offline message in Redis for user {}: ", userId, e);
        }
    }

    private void deliverOfflineMessages(Channel channel, String username) {
        BusinessThreadPool.submit(() -> {
            try {
                RedisTemplate<String, Object> redis = RedisUtil.getRedis();
                if (redis == null) return;

                String redisKey = "OFFLINE_MSG_" + username;
                List<Object> messages = redis.opsForList().range(redisKey, 0, -1);
                if (messages == null || messages.isEmpty()) return;

                log.info("Delivering {} offline messages to user {}", messages.size(), username);

                for (Object rawBody : messages) {
                    byte[] body;
                    if (rawBody instanceof byte[]) {
                        body = (byte[]) rawBody;
                    } else {
                        // Redis might store as String depending on serializer config
                        body = rawBody.toString().getBytes();
                    }

                    Message offlineMsg = new Message();
                    offlineMsg.setCommand(ProtocolConstants.COMMAND_CHAT);
                    offlineMsg.setBody(body);
                    channel.writeAndFlush(offlineMsg);
                }

                // Clear the offline queue after delivery
                redis.delete(redisKey);
                log.info("All offline messages delivered and cleared for user {}", username);

            } catch (Exception e) {
                log.error("Error delivering offline messages for user {}: ", username, e);
            }
        });
    }

    // ========== LIFECYCLE ==========

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        MetricsMonitor.decrementConnections();

        // Clean up session when connection is closed (normal disconnect or heartbeat timeout)
        String username = ctx.channel().attr(SessionManager.ATTR_USER).get();
        if (username != null) {
            log.info("Connection closed for user {}, cleaning up session", username);
            SessionManager.removeSession(ctx.channel());

            // Unregister user from Redis cross-server routing
            RedisRouter.unregisterUser(username);

            // Remove from all ChannelGroups (Netty handles this automatically when channel closes,
            // but we keep the membership in GROUP_MEMBERS for reconnection)
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in handler pipeline: ", cause);
        SessionManager.removeSession(ctx.channel());
        ctx.close();
    }
}

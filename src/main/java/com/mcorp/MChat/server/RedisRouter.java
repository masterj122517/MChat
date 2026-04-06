package com.mcorp.MChat.server;

import com.mcorp.MChat.protocol.Message;
import com.mcorp.MChat.protocol.ProtocolConstants;
import com.mcorp.MChat.utils.RedisUtil;
import io.netty.channel.Channel;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Redis-based routing layer for cross-server communication.
 *
 * When user A is on Server1 and user B is on Server2:
 * 1. Server1 checks if user B is local (SessionManager) -> not found
 * 2. Server1 publishes the message to Redis channel "mchat:route:{targetUserId}"
 * 3. Server2 (subscribed to that channel) receives the message and delivers locally
 *
 * Additionally, each server registers its online users in Redis so other servers
 * can check if a user is online somewhere in the cluster.
 */
@Slf4j
@Component
public class RedisRouter {

    private static final String CHANNEL_PREFIX = "mchat:route:";
    private static final String ONLINE_KEY_PREFIX = "mchat:online:";

    @Value("${mchat.server.id:#{T(java.util.UUID).randomUUID().toString().substring(0,8)}}")
    private String serverId;

    private RedisMessageListenerContainer listenerContainer;

    @PostConstruct
    public void init() {
        log.info("RedisRouter initialized for server: {}", serverId);
    }

    /**
     * Try to route a message to a user that is NOT on this server.
     * Returns true if the message was published to Redis (user may be on another server).
     * Returns false if Redis is unavailable or the user is not registered anywhere.
     */
    public static boolean routeToRemote(String targetUserId, Message msg) {
        try {
            RedisTemplate<String, Object> redis = RedisUtil.getRedis();
            if (redis == null) return false;

            // Check if the user is online on any server in the cluster
            String onlineServer = (String) redis.opsForValue().get(ONLINE_KEY_PREFIX + targetUserId);
            if (onlineServer == null) {
                // User not online on any server
                return false;
            }

            // Publish to the target server's routing channel
            String channel = CHANNEL_PREFIX + targetUserId;
            redis.convertAndSend(channel, msg.getBody());
            log.info("Routed message for user {} via Redis Pub/Sub", targetUserId);
            return true;

        } catch (Exception e) {
            log.error("Failed to route message via Redis for user {}: ", targetUserId, e);
            return false;
        }
    }

    /**
     * Register a user as online on this server.
     * Called when a user logs in.
     */
    public static void registerUserOnline(String username) {
        try {
            RedisTemplate<String, Object> redis = RedisUtil.getRedis();
            if (redis == null) return;
            redis.opsForValue().set(ONLINE_KEY_PREFIX + username, "online");
            log.debug("Registered user {} as online in Redis", username);
        } catch (Exception e) {
            log.error("Failed to register user {} online in Redis: ", username, e);
        }
    }

    /**
     * Unregister a user when they go offline.
     * Called when a user disconnects.
     */
    public static void unregisterUser(String username) {
        try {
            RedisTemplate<String, Object> redis = RedisUtil.getRedis();
            if (redis == null) return;
            redis.delete(ONLINE_KEY_PREFIX + username);
            log.debug("Unregistered user {} from Redis", username);
        } catch (Exception e) {
            log.error("Failed to unregister user {} from Redis: ", username, e);
        }
    }

    /**
     * Subscribe to Redis channel for a specific user on this server.
     * When a message arrives via Redis Pub/Sub, deliver it locally.
     */
    public static void subscribeForUser(String username, RedisConnectionFactory connectionFactory) {
        try {
            RedisMessageListenerContainer container = new RedisMessageListenerContainer();
            container.setConnectionFactory(connectionFactory);

            MessageListener listener = (message, pattern) -> {
                try {
                    byte[] body = message.getBody();
                    Channel channel = SessionManager.getSession(username);
                    if (channel != null && channel.isActive()) {
                        Message chatMsg = new Message();
                        chatMsg.setCommand(ProtocolConstants.COMMAND_CHAT);
                        chatMsg.setBody(body);
                        channel.writeAndFlush(chatMsg);
                        log.info("Delivered cross-server message to local user {}", username);
                    }
                } catch (Exception e) {
                    log.error("Error delivering cross-server message to user {}: ", username, e);
                }
            };

            container.addMessageListener(listener, new ChannelTopic(CHANNEL_PREFIX + username));
            container.afterPropertiesSet();
            container.start();
            log.debug("Subscribed to Redis channel for user {}", username);

        } catch (Exception e) {
            log.error("Failed to subscribe Redis channel for user {}: ", username, e);
        }
    }
}

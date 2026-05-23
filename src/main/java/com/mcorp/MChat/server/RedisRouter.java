package com.mcorp.MChat.server;

import com.mcorp.MChat.protocol.Message;
import com.mcorp.MChat.protocol.ProtocolConstants;
import com.mcorp.MChat.utils.RedisUtil;
import io.netty.channel.Channel;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class RedisRouter {

    private static final String CHANNEL_PREFIX = "mchat:route:";
    private static final String ONLINE_KEY_PREFIX = "mchat:online:";

    @Value("${mchat.server.id:#{T(java.util.UUID).randomUUID().toString().substring(0,8)}}")
    private String serverId;

    @Autowired
    private RedisMessageListenerContainer sharedListenerContainer;

    private static RedisMessageListenerContainer staticListenerContainer;

    @PostConstruct
    public void init() {
        staticListenerContainer = sharedListenerContainer;
        log.info("RedisRouter initialized for server: {}", serverId);
    }

    public static boolean routeToRemote(String targetUserId, Message msg) {
        try {
            RedisTemplate<String, Object> redis = RedisUtil.getRedis();
            if (redis == null) return false;

            String onlineServer = (String) redis.opsForValue().get(ONLINE_KEY_PREFIX + targetUserId);
            if (onlineServer == null) {
                return false;
            }

            byte[] channelBytes = (CHANNEL_PREFIX + targetUserId).getBytes(StandardCharsets.UTF_8);
            byte[] fullMessage = msg.getBody();
            redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) connection -> {
                connection.publish(channelBytes, fullMessage);
                return null;
            });

            log.info("Routed message for user {} via Redis Pub/Sub", targetUserId);
            return true;

        } catch (Exception e) {
            log.error("Failed to route message via Redis for user {}: ", targetUserId, e);
            return false;
        }
    }

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

    public static void subscribeForUser(String username) {
        try {
            if (staticListenerContainer == null) {
                log.error("Shared RedisMessageListenerContainer is not available, cannot subscribe for user {}", username);
                return;
            }

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

            staticListenerContainer.addMessageListener(listener, new ChannelTopic(CHANNEL_PREFIX + username));
            log.debug("Subscribed to Redis channel for user {}", username);

        } catch (Exception e) {
            log.error("Failed to subscribe Redis channel for user {}: ", username, e);
        }
    }
}

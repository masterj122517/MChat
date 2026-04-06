package com.mcorp.MChat.server;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SessionManager {
    // 1. 核心容器
    private static final ConcurrentHashMap<String, Channel> USER_SESSIONS = new ConcurrentHashMap<>();

    // 2. 定义 AttributeKey：把用户名刻在连接上
    public static final AttributeKey<String> ATTR_USER = AttributeKey.valueOf("username");

    public static void addSession(String username, Channel channel) {
        // 3. 处理重复登录：如果该用户已在线，踢掉旧连接
        Channel oldChannel = USER_SESSIONS.get(username);
        if (oldChannel != null && oldChannel != channel) {
            log.warn("检测到用户 {} 重复登录，正在踢掉旧连接。", username);
            oldChannel.close();
        }

        // 4. 绑定关系
        USER_SESSIONS.put(username, channel);
        channel.attr(ATTR_USER).set(username); // 关键：反向绑定的秘诀

        log.info("用户 {} 已上线，当前在线总数: {}", username, USER_SESSIONS.size());
    }

    // 5. 增强版移除：只需要 Channel 就能移除
    public static void removeSession(Channel channel) {
        String username = channel.attr(ATTR_USER).get();
        if (username != null) {
            USER_SESSIONS.remove(username);
            log.info("用户 {} 已下线，当前在线总数: {}", username, USER_SESSIONS.size());
        }
    }

    public static Channel getSession(String username) {
        return USER_SESSIONS.get(username);
    }
}